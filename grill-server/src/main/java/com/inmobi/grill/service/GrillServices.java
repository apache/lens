package com.inmobi.grill.service;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.ws.rs.WebApplicationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import com.inmobi.grill.api.GrillConfConstants;
import com.inmobi.grill.server.api.GrillService;

public class GrillServices {
  public static final Log LOG = LogFactory.getLog(GrillServices.class);

  static {
    Configuration.addDefaultResource("grill-default.xml");
    Configuration.addDefaultResource("grill-site.xml");
  }

  private static final GrillServices INSTANCE = new GrillServices();
  private Configuration conf = new Configuration();
  private boolean inited = false;

  private GrillServices() {
  }

  public synchronized void initServices() {
      if (!inited) {
        String[] serviceNames = conf.getStrings(GrillConfConstants.GRILL_SERVICE_NAMES);
        for (String sName : serviceNames) {
          try {
            Class<? extends GrillService> serviceClass = conf.getClass(
                GrillConfConstants.getServiceImplConfKey(sName), null,
                GrillService.class);
            LOG.info("Starting " + sName + " service with " + serviceClass);
            GrillService service = ReflectionUtils.newInstance(serviceClass, conf);
            service.init();
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
