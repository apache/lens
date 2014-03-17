package com.inmobi.grill.server.api;

public class GrillConfConstants {

  public static final String PREPARE_ON_EXPLAIN = "grill.doprepare.on.explain";

  public static final Boolean DEFAULT_PREPARE_ON_EXPLAIN = true;

  public static final String ENGINE_DRIVER_CLASSES = "grill.drivers";

  public static final String STORAGE_COST = "grill.storage.cost";

  public static final String GRILL_PERSISTENT_RESULT_SET = "grill.persistent.resultset";

  public static final String GRILL_RESULT_SET_PARENT_DIR = "grill.result.parent.dir";

  public static final String GRILL_SERVICE_NAMES = "grill.servicenames";

  public static final String GRILL_PFX = "grill.";

  public static final String GRILL_SERVICE_IMPL_SFX = ".service.impl";
  public static final String GRILL_QUERY_STATE_LOGGER_ENABLED = "grill.query.state.logger.enabled";
  public static final String EVENT_SERVICE_THREAD_POOL_SIZE = "grill.event.service.thread.pool.size";
  
  public static final String GRILL_SERVER_BASE_URL = "grill.server.base.url";
  public static final String DEFAULT_GRILL_SERVER_BASE_URL = "http://0.0.0.0:9999/";

  public static String getServiceImplConfKey(String sName) {
    return GRILL_PFX + sName + GRILL_SERVICE_IMPL_SFX;
  }
}
