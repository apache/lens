package com.inmobi.grill.server.api;

/*
 * #%L
 * Grill API for server and extensions
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

  public static final String GRILL_SERVER_RESTART_ENABLED = "grill.server.restart.enabled";
  public static final boolean DEFAULT_GRILL_SERVER_RESTART_ENABLED = true;
  public static final String GRILL_SERVER_PERSIST_LOCATION = "grill.server.persist.location";
  public static final String DEFAULT_GRILL_SERVER_PERSIST_LOCATION = "file:///tmp/grillserver";
  public static final String GRILL_SERVER_RECOVER_ON_RESTART = "grill.server.recover.onrestart";
  public static final boolean DEFAULT_GRILL_SERVER_RECOVER_ON_RESTART = true;

  public static String getServiceImplConfKey(String sName) {
    return GRILL_PFX + sName + GRILL_SERVICE_IMPL_SFX;
  }

  public static final String GRILL_RESULT_SET_PARENT_DIR_DEFAULT = "/tmp/grillreports";

  public static final String GRILL_OUTPUT_DIRECTORY_FORMAT = "grill.result.output.dir.format";

  public static final String GRILL_ADD_INSERT_OVEWRITE = "grill.add.insert.overwrite";

  public static final String ENABLE_CONSOLE_METRICS = "grill.enable.console.metrics";
  public static final String ENABLE_GANGLIA_METRICS = "grill.enable.ganglia.metrics";
  public final static String GANGLIA_SERVERNAME = "grill.metrics.ganglia.host";
  public final static String GANGLIA_PORT = "grill.metrics.ganglia.port";
  public final static String REPORTING_PERIOD = "grill.metrics.reporting.period";

  public static final String GRILL_SERVER_MODE = "grill.server.mode";
  public final static String DEFAULT_GRILL_SERVER_MODE = "OPEN";
}
