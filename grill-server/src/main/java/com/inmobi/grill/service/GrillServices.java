package com.inmobi.grill.service;

import java.lang.reflect.Constructor;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.ws.rs.WebApplicationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIService;

import com.inmobi.grill.api.GrillConfConstants;
import com.inmobi.grill.service.session.GrillSessionImpl;

public class GrillServices {
  public static final Log LOG = LogFactory.getLog(GrillServices.class);

  static {
    Configuration.addDefaultResource("grill-default.xml");
    Configuration.addDefaultResource("grill-site.xml");
  }

  private static final GrillServices INSTANCE = new GrillServices();
  private HiveConf conf = new HiveConf();
  private boolean inited = false;

  private GrillServices() {
  }

  public synchronized void initServices() {
      if (!inited) {
        CLIService cliService = new CLIService();
        conf.setVar(HiveConf.ConfVars.HIVE_SESSION_IMPL_CLASSNAME, GrillSessionImpl.class.getCanonicalName());
        cliService.init(conf);
        cliService.start();
        String[] serviceNames = conf.getStrings(GrillConfConstants.GRILL_SERVICE_NAMES);
        for (String sName : serviceNames) {
          try {
            Class<? extends GrillService> serviceClass = conf.getClass(
                GrillConfConstants.getServiceImplConfKey(sName), null,
                GrillService.class);
            LOG.info("Starting " + sName + " service with " + serviceClass);
            Constructor<?> constructor = serviceClass.getConstructor(CLIService.class);
            GrillService service  = (GrillService) constructor.newInstance(new Object[]
                {cliService});
            service.init(conf);
            services.put(sName, service);
            service.start();
          } catch (Exception e) {
            throw new WebApplicationException(e);
          }
        }
        inited = true;
    }
  }

  public static GrillServices get() {
    return INSTANCE;
  }

  private final Map<String, GrillService> services =
      new LinkedHashMap<String, GrillService>();

  public synchronized void stopAll() {
      for (GrillService service : services.values()) {
        try {
          service.stop();
        } catch (Exception e) {
          throw new WebApplicationException(e);
        }
      }
  }

  @SuppressWarnings("unchecked")
  public <T extends GrillService> T getService(String sName) {
    return (T) services.get(sName);
  }
}
