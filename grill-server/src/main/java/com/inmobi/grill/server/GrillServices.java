package com.inmobi.grill.server;

/*
 * #%L
 * Grill Server
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.WebApplicationException;

import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.events.GrillEventService;
import com.inmobi.grill.server.api.metrics.MetricsService;
import com.inmobi.grill.server.session.GrillSessionImpl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.Service;
import org.apache.hive.service.Service.STATE;
import org.apache.hive.service.cli.CLIService;

public class GrillServices extends CompositeService {
  public static final Log LOG = LogFactory.getLog(GrillServices.class);

  static {
    Configuration.addDefaultResource("grill-default.xml");
    Configuration.addDefaultResource("grill-site.xml");
  }

  public static final String GRILL_SERVICES_NAME = "grill_services";
  private static final GrillServices INSTANCE = new GrillServices(GRILL_SERVICES_NAME);
  private HiveConf conf;
  private CLIService cliService;
  private final Map<String, Service> services = new LinkedHashMap<String, Service>();
  private final List<GrillService> grillServices = new ArrayList<GrillService>();
  private Path persistDir;

  public GrillServices(String name) {
    super(name);
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    if (getServiceState() == STATE.NOTINITED) {
      conf = hiveConf;
      conf.setVar(HiveConf.ConfVars.HIVE_SESSION_IMPL_CLASSNAME, GrillSessionImpl.class.getCanonicalName());
      cliService = new CLIService();

      // Add default services
      addService(cliService);
      addService(new EventServiceImpl(GrillEventService.NAME));
      addService(new MetricsServiceImpl(MetricsService.NAME));
      
      // Add configured services, these are instances of GrillService which need a CLIService instance
      // for session management
      String[] serviceNames = conf.getStrings(GrillConfConstants.GRILL_SERVICE_NAMES);
      for (String sName : serviceNames) {
        try {
          Class<? extends GrillService> serviceClass = conf.getClass(
              GrillConfConstants.getServiceImplConfKey(sName), null,
              GrillService.class);
          LOG.info("Adding " + sName + " service with " + serviceClass);
          Constructor<?> constructor = serviceClass.getConstructor(CLIService.class);
          GrillService service  = (GrillService) constructor.newInstance(new Object[]
              {cliService});
          addService(service);
          grillServices.add(service);
        } catch (Exception e) {
          LOG.warn("Could not add service:" + sName, e);
          throw new WebApplicationException(e);
        }
      }

      for (Service svc : getServices()) {
        services.put(svc.getName(), svc);
      }

      // This will init all services in the order they were added
      super.init(conf);

      //setup persisted state
      String persistPathStr = conf.get(GrillConfConstants.GRILL_SERVER_PERSIST_LOCATION,
          GrillConfConstants.DEFAULT_GRILL_SERVER_PERSIST_LOCATION);
      persistDir = new Path(persistPathStr);
      try {
        setupPersistedState();
      } catch (Exception e) {
        LOG.error("Could not setup persisted state", e);
        throw new WebApplicationException(e);
      }

      LOG.info("Initialized grill services: " + services.keySet().toString());
    }
  }

  public synchronized void start() {
    if (getServiceState() != STATE.STARTED) {
      super.start();
    }
  }

  private void setupPersistedState() throws IOException, ClassNotFoundException {
    if (conf.getBoolean(GrillConfConstants.GRILL_SERVER_RESTART_ENABLED,
        GrillConfConstants.DEFAULT_GRILL_SERVER_RESTART_ENABLED)) { 
      FileSystem fs = persistDir.getFileSystem(conf);

      for (GrillService service : grillServices) {
        FSDataInputStream fsin;
        try {
          fsin = fs.open(getServicePersistPath(service));
        } catch (FileNotFoundException fe) {
          LOG.warn("No persist path available for service:" + service.getName());
          continue;
        }
        int length = fsin.available();
        byte[] bytes = new byte[length];
        fsin.readFully(bytes);
        ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
        service.readExternal(new ObjectInputStream(byteIn));
        byteIn.close();
        fsin.close();
      }
    }
  }
  private void persistGrillServiceState() throws IOException {
    FileSystem fs = persistDir.getFileSystem(conf);

    for (GrillService service : grillServices) {
      Path serviceWritePath = new Path(persistDir, service.getName() + ".out");
      OutputStream fsout = fs.create(serviceWritePath);
      ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
      service.writeExternal(new ObjectOutputStream(byteOut));
      fsout.write(byteOut.toByteArray());
      byteOut.close();
      fsout.close();
      Path servicePath = getServicePersistPath(service);
      fs.rename(serviceWritePath, servicePath);
    }
  }

  private Path getServicePersistPath(GrillService service) {
    return new Path(persistDir, service.getName() + ".final");
  }

  public synchronized void stop() {
    if (getServiceState() != STATE.STOPPED) {
      try {
        // persist all the services
        persistGrillServiceState();
      } catch (IOException e) {
        LOG.error("Could not persist server state", e);
      }
      super.stop();
    }
  }

  public STATE getServiceState() {
    return super.getServiceState();
  }

  public static GrillServices get() {
    return INSTANCE;
  }

  @SuppressWarnings("unchecked")
  public <T extends Service> T getService(String sName) {
    return (T) services.get(sName);
  }

  public List<GrillService> getGrillServices() {
    return grillServices;
  }
}
