/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.util.*;

import lombok.Getter;
import lombok.Setter;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.Service;
import org.apache.hive.service.cli.CLIService;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.ServiceProvider;
import org.apache.lens.server.api.events.LensEventService;
import org.apache.lens.server.api.metrics.MetricsService;
import org.apache.lens.server.session.LensSessionImpl;
import org.apache.lens.server.stats.StatisticsService;
import org.apache.lens.server.user.UserConfigLoaderFactory;

/**
 * The Class LensServices.
 */
public class LensServices extends CompositeService implements ServiceProvider {

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(LensServices.class);

  /** The Constant LENS_SERVICES_NAME. */
  public static final String LENS_SERVICES_NAME = "lens_services";

  /** The instance. */
  private static LensServices INSTANCE = new LensServices(LENS_SERVICES_NAME);

  /** The conf. */
  private HiveConf conf;

  /** The cli service. */
  private CLIService cliService;

  /** The services. */
  private final Map<String, Service> services = new LinkedHashMap<String, Service>();

  /** The lens services. */
  private final List<LensService> lensServices = new ArrayList<LensService>();

  /** The persist dir. */
  private Path persistDir;

  /** The stopping. */
  private boolean stopping = false;

  /** The snap shot interval. */
  private long snapShotInterval;

  /** The service mode. */
  @Getter
  @Setter
  private SERVICE_MODE serviceMode;

  /** The timer. */
  private Timer timer;

  /**
   * The Enum SERVICE_MODE.
   */
  public enum SERVICE_MODE {

    /** The read only. */
    READ_ONLY, // All requests on sesssion resource and Only GET requests on all other resources
    /** The metastore readonly. */
    METASTORE_READONLY, // Only GET requests on metastore service and
    // all other requests on other services are accepted
    /** The metastore nodrop. */
    METASTORE_NODROP, // DELETE requests on metastore are not accepted
    /** The open. */
    OPEN // All requests are accepted
  };

  /**
   * Instantiates a new lens services.
   *
   * @param name
   *          the name
   */
  public LensServices(String name) {
    super(name);
  }

  // This is only for test, to simulate a restart of the server
  static void setInstance(LensServices newInstance) {
    INSTANCE = newInstance;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hive.service.CompositeService#init(org.apache.hadoop.hive.conf.HiveConf)
   */
  @SuppressWarnings("unchecked")
  @Override
  public synchronized void init(HiveConf hiveConf) {
    if (getServiceState() == STATE.NOTINITED) {
      conf = hiveConf;
      conf.addResource("lensserver-default.xml");
      conf.addResource("lens-site.xml");
      conf.setVar(HiveConf.ConfVars.HIVE_SESSION_IMPL_CLASSNAME, LensSessionImpl.class.getCanonicalName());
      serviceMode = conf.getEnum(LensConfConstants.SERVER_MODE,
          SERVICE_MODE.valueOf(LensConfConstants.DEFAULT_SERVER_MODE));
      cliService = new CLIService();

      // Add default services
      addService(cliService);
      addService(new EventServiceImpl(LensEventService.NAME));
      addService(new MetricsServiceImpl(MetricsService.NAME));
      addService(new StatisticsService(StatisticsService.STATS_SVC_NAME));

      // Add configured services, these are instances of LensService which need a CLIService instance
      // for session management
      String[] serviceNames = conf.getStrings(LensConfConstants.SERVICE_NAMES);
      for (String sName : serviceNames) {
        try {

          String serviceClassName = conf.get(LensConfConstants.getServiceImplConfKey(sName));

          if (StringUtils.isBlank(serviceClassName)) {
            LOG.warn("Invalid class for service " + sName + " class=" + serviceClassName);
            continue;
          }

          Class<?> cls = Class.forName(serviceClassName);

          if (LensService.class.isAssignableFrom(cls)) {
            Class<? extends LensService> serviceClass = (Class<? extends LensService>) cls;
            LOG.info("Adding " + sName + " service with " + serviceClass);
            Constructor<?> constructor = serviceClass.getConstructor(CLIService.class);
            LensService service = (LensService) constructor.newInstance(new Object[] { cliService });
            addService(service);
            lensServices.add(service);
          } else if (Service.class.isAssignableFrom(cls)) {
            Class<? extends Service> serviceClass = (Class<? extends Service>) cls;
            // Assuming default constructor
            Service svc = serviceClass.newInstance();
            addService(svc);
          } else {
            LOG.warn("Unsupported service class " + serviceClassName + " for service " + sName);
          }
        } catch (Exception e) {
          LOG.warn("Could not add service:" + sName, e);
          throw new RuntimeException("Could not add service:" + sName, e);
        }
      }

      for (Service svc : getServices()) {
        services.put(svc.getName(), svc);
      }

      // This will init all services in the order they were added
      super.init(conf);

      // setup persisted state
      String persistPathStr = conf.get(LensConfConstants.SERVER_STATE_PERSIST_LOCATION,
          LensConfConstants.DEFAULT_SERVER_STATE_PERSIST_LOCATION);
      persistDir = new Path(persistPathStr);
      try {
        setupPersistedState();
      } catch (Exception e) {
        LOG.error("Could not recover from persisted state", e);
        throw new RuntimeException("Could not recover from persisted state", e);
      }
      snapShotInterval = conf.getLong(LensConfConstants.SERVER_SNAPSHOT_INTERVAL,
          LensConfConstants.DEFAULT_SERVER_SNAPSHOT_INTERVAL);
      LOG.info("Initialized services: " + services.keySet().toString());
      UserConfigLoaderFactory.init(conf);
      timer = new Timer("lens-server-snapshotter", true);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hive.service.CompositeService#start()
   */
  public synchronized void start() {
    if (getServiceState() != STATE.STARTED) {
      super.start();
    }
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          persistLensServiceState();
          LOG.info("SnapShot of Lens Services created");
        } catch (IOException e) {
          LOG.warn("Unable to persist lens server state", e);
        }
      }
    }, snapShotInterval, snapShotInterval);
  }

  /**
   * Setup persisted state.
   *
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   * @throws ClassNotFoundException
   *           the class not found exception
   */
  private void setupPersistedState() throws IOException, ClassNotFoundException {
    if (conf.getBoolean(LensConfConstants.SERVER_RECOVER_ON_RESTART,
        LensConfConstants.DEFAULT_SERVER_RECOVER_ON_RESTART)) {
      FileSystem fs = persistDir.getFileSystem(conf);

      for (LensService service : lensServices) {
        ObjectInputStream in = null;
        try {
          try {
            in = new ObjectInputStream(fs.open(getServicePersistPath(service)));
          } catch (FileNotFoundException fe) {
            LOG.warn("No persist path available for service:" + service.getName());
            continue;
          }
          service.readExternal(in);
          LOG.info("Recovered service " + service.getName() + " from persisted state");
        } finally {
          if (in != null) {
            in.close();
          }
        }
      }
    }
  }

  /**
   * Persist lens service state.
   *
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private synchronized void persistLensServiceState() throws IOException {
    if (conf.getBoolean(LensConfConstants.SERVER_RESTART_ENABLED, LensConfConstants.DEFAULT_SERVER_RESTART_ENABLED)) {
      FileSystem fs = persistDir.getFileSystem(conf);
      LOG.info("Persisting server state in " + persistDir);

      for (LensService service : lensServices) {
        LOG.info("Persisting state of service:" + service.getName());
        Path serviceWritePath = new Path(persistDir, service.getName() + ".out");
        ObjectOutputStream out = null;
        try {
          out = new ObjectOutputStream(fs.create(serviceWritePath));
          service.writeExternal(out);
        } finally {
          if (out != null) {
            out.close();
          }
        }
        Path servicePath = getServicePersistPath(service);
        fs.rename(serviceWritePath, servicePath);
        LOG.info("Persisted service " + service.getName() + " to " + servicePath);
      }
    } else {
      LOG.info("Server restart is not enabled. Not persisting the server state");
    }
  }

  /**
   * Gets the service persist path.
   *
   * @param service
   *          the service
   * @return the service persist path
   */
  private Path getServicePersistPath(LensService service) {
    return new Path(persistDir, service.getName() + ".final");
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hive.service.CompositeService#stop()
   */
  public synchronized void stop() {
    if (getServiceState() != STATE.STOPPED) {
      LOG.info("Stopping lens server");
      stopping = true;
      for (LensService service : lensServices) {
        service.prepareStopping();
      }
      timer.cancel();
      try {
        // persist all the services
        persistLensServiceState();
      } catch (IOException e) {
        LOG.error("Could not persist server state", e);
        throw new IllegalStateException(e);
      } finally {
        super.stop();
      }
    }
  }

  public STATE getServiceState() {
    return super.getServiceState();
  }

  public boolean isStopping() {
    return stopping;
  }

  /**
   * Gets the.
   *
   * @return the lens services
   */
  public static LensServices get() {
    return INSTANCE;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.ServiceProvider#getService(java.lang.String)
   */
  @Override
  @SuppressWarnings("unchecked")
  public <T extends Service> T getService(String sName) {
    return (T) services.get(sName);
  }

  public List<LensService> getLensServices() {
    return lensServices;
  }
}
