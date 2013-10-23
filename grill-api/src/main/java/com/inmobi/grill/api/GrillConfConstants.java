package com.inmobi.grill.api;

import org.apache.hadoop.conf.Configuration;

public class GrillConfConstants {

  static {
    Configuration.addDefaultResource("grill-default.xml");
    Configuration.addDefaultResource("grill-site.xml");
  }
  public static final String PREPARE_ON_EXPLAIN = "grill.doprepare.on.explain";

  public static final Boolean DEFAULT_PREPARE_ON_EXPLAIN = true;

  public static final String ENGINE_DRIVER_CLASSES = "grill.drivers";

  public static final String STORAGE_COST = "grill.storage.cost";

  public static final String GRILL_PERSISTENT_RESULT_SET = "grill.persistent.resultset";

  public static final String GRILL_RESULT_SET_PARENT_DIR = "grill.result.parent.dir";

}
