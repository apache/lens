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
  public static final String GRILL_SESSION_TIMEOUT_SECONDS = "grill.server.session.timeout.seconds";
  public static final long GRILL_SESSION_TIMEOUT_SECONDS_DEFAULT = 1440 * 60; // Default is one day
  public static final String GRILL_SERVER_UI_URI = "grill.server.ui.base.uri";
  public static final String DEFAULT_GRILL_SERVER_UI_URI = "http://0.0.0.0:19999/";
  public static final String GRILL_SERVER_UI_STATIC_DIR = "grill.server.ui.static.dir";
  public static final String DEFAULT_GRILL_SERVER_UI_STATIC_DIR = "webapp/grill-server/static";
  public static final String GRILL_SERVER_UI_ENABLE_CACHING = "grill.server.ui.enable.caching";
  public static final boolean DEFAULT_GRILL_SERVER_UI_ENABLE_CACHING = true;

  // These properties should be set in native tables if their underlying DB or table names are different
  public static final String GRILL_NATIVE_DB_NAME = "grill.native.db.name";
  public static final String GRILL_NATIVE_TABLE_NAME = "grill.native.table.name";

  public static String getServiceImplConfKey(String sName) {
    return GRILL_PFX + sName + GRILL_SERVICE_IMPL_SFX;
  }

  public static final String ENABLE_CONSOLE_METRICS = "grill.enable.console.metrics";
  public static final String ENABLE_GANGLIA_METRICS = "grill.enable.ganglia.metrics";
  public final static String GANGLIA_SERVERNAME = "grill.metrics.ganglia.host";
  public final static String GANGLIA_PORT = "grill.metrics.ganglia.port";
  public final static String REPORTING_PERIOD = "grill.metrics.reporting.period";

  public static final String GRILL_SERVER_MODE = "grill.server.mode";
  public final static String DEFAULT_GRILL_SERVER_MODE = "OPEN";

  // resultset output options
  public static final String GRILL_PERSISTENT_RESULT_SET = "grill.persistent.resultset";

  public static final boolean DEFAULT_PERSISTENT_RESULT_SET = false;

  public static final String GRILL_RESULT_SET_PARENT_DIR = "grill.result.parent.dir";

  public static final String GRILL_RESULT_SET_PARENT_DIR_DEFAULT = "/tmp/grillreports";

  public static final String GRILL_ADD_INSERT_OVEWRITE = "grill.add.insert.overwrite";

  public static final boolean DEFAULT_ADD_INSERT_OVEWRITE = true;

  public static final String QUERY_PERSISTENT_RESULT_INDRIVER = "grill.persistent.resultset.indriver";

  public static final boolean DEFAULT_DRIVER_PERSISTENT_RESULT_SET = true;

  public static final String QUERY_HDFS_OUTPUT_PATH = "grill.query.hdfs.output.path";

  public static final String DEFAULT_HDFS_OUTPUT_PATH = "hdfsout";

  public static final String QUERY_OUTPUT_DIRECTORY_FORMAT = "grill.result.output.dir.format";

  public static final String QUERY_OUTPUT_SQL_FORMAT = "grill.result.output.sql.format";

  public static final String QUERY_OUTPUT_FORMATTER = "grill.query.output.formatter";

  public static final String DEFAULT_INMEMORY_OUTPUT_FORMATTER = "com.inmobi.grill.lib.query.FileSerdeFormatter";

  public static final String DEFAULT_PERSISTENT_OUTPUT_FORMATTER = "com.inmobi.grill.lib.query.FilePersistentFormatter";

  public static final String QUERY_OUTPUT_SERDE = "grill.result.output.serde";

  public static final String DEFAULT_OUTPUT_SERDE = "com.inmobi.grill.lib.query.CSVSerde";

  public static final String QUERY_OUTPUT_FILE_EXTN = "grill.query.output.file.extn";

  public static final String DEFAULT_OUTPUT_FILE_EXTN = ".csv";

  public static final String QUERY_OUTPUT_CHARSET_ENCODING = "grill.query.output.charset.encoding";

  public static final String DEFAULT_OUTPUT_CHARSET_ENCODING = "UTF-8";

  public static final String QUERY_OUTPUT_ENABLE_COMPRESSION = "grill.query.output.enable.compression";

  public static final boolean DEFAULT_OUTPUT_ENABLE_COMPRESSION = false;

  public static final String QUERY_OUTPUT_COMPRESSION_CODEC = "grill.query.output.compression.codec";

  public static final String DEFAULT_OUTPUT_COMPRESSION_CODEC = "org.apache.hadoop.io.compress.GzipCodec";

  public static final String QUERY_OUTPUT_WRITE_HEADER = "grill.query.output.write.header";

  public static final boolean DEFAULT_OUTPUT_WRITE_HEADER = false;

  public static final String QUERY_OUTPUT_HEADER = "grill.query.output.header";

  public static final String QUERY_OUTPUT_WRITE_FOOTER = "grill.query.output.write.footer";

  public static final boolean DEFAULT_OUTPUT_WRITE_FOOTER = false;

  public static final String QUERY_OUTPUT_FOOTER = "grill.query.output.footer";

  public static final String RESULT_FORMAT_SIZE_THRESHOLD = "grill.query.result.size.format.threshold";

  public static final long DEFAULT_RESULT_FORMAT_SIZE_THRESHOLD = 10737418240L; //10GB

  public static final String RESULT_SPLIT_INTO_MULTIPLE = "grill.query.result.split.multiple";

  public static final boolean DEFAULT_RESULT_SPLIT_INTO_MULTIPLE = false;

  public static final String RESULT_SPLIT_MULTIPLE_MAX_ROWS = "grill.query.result.split.multiple.maxrows";

  public static final long DEFAULT_RESULT_SPLIT_MULTIPLE_MAX_ROWS = 100000;

  public static final String RESULT_FS_READ_URL = "grill.query.result.fs.read.url";

  public static final String AUX_JARS = "grill.aux.jars";

}
