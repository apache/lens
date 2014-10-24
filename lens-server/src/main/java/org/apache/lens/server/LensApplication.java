package org.apache.lens.server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.lens.server.api.LensConfConstants;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;

/**
 * The Class LensApplication.
 */
@ApplicationPath("/")
public class LensApplication extends Application {

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(LensApplication.class);

  /** The conf. */
  public static HiveConf conf = LensServerConf.get();

  @Override
  public Set<Class<?>> getClasses() {

    final Set<Class<?>> classes = new HashSet<Class<?>>();

    String[] resourceNames = conf.getStrings(LensConfConstants.WS_RESOURCE_NAMES);
    String[] featureNames = conf.getStrings(LensConfConstants.WS_FEATURE_NAMES);
    String[] listenerNames = conf.getStrings(LensConfConstants.WS_LISTENER_NAMES);
    String[] filterNames = conf.getStrings(LensConfConstants.WS_FILTER_NAMES);

    // register root resource
    for (String rName : resourceNames) {
      Class wsResourceClass = conf.getClass(LensConfConstants.getWSResourceImplConfKey(rName), null);
      classes.add(wsResourceClass);
      LOG.info("Added resource " + wsResourceClass);
    }
    for (String fName : featureNames) {
      Class wsFeatureClass = conf.getClass(LensConfConstants.getWSFeatureImplConfKey(fName), null);
      classes.add(wsFeatureClass);
      LOG.info("Added feature " + wsFeatureClass);
    }
    for (String lName : listenerNames) {
      Class wsListenerClass = conf.getClass(LensConfConstants.getWSListenerImplConfKey(lName), null);
      classes.add(wsListenerClass);
      LOG.info("Added listener " + wsListenerClass);
    }
    for (String filterName : filterNames) {
      Class wsFilterClass = conf.getClass(LensConfConstants.getWSFilterImplConfKey(filterName), null);
      classes.add(wsFilterClass);
      LOG.info("Added filter " + wsFilterClass);
    }
    return classes;
  }

}
