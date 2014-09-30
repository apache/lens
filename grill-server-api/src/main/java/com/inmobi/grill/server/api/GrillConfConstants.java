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
  public static final String GRILL_WS_RESOURCE_NAMES = "grill.ws.resourcenames";
  public static final String GRILL_WS_LISTENER_NAMES = "grill.ws.listenernames";
  public static final String GRILL_WS_FILTER_NAMES = "grill.ws.filternames";
  public static final String GRILL_WS_FEATURE_NAMES = "grill.ws.featurenames";

  public static final String GRILL_PFX = "grill.";

  public static final String GRILL_SERVICE_IMPL_SFX = ".service.impl";
  public static final String GRILL_WS_RESOURCE_IMPL_SFX = ".ws.resource.impl";
  public static final String GRILL_WS_FEATURE_IMPL_SFX = ".ws.feature.impl";
  public static final String GRILL_WS_LISTENER_IMPL_SFX = ".ws.listener.impl";
  public static final String GRILL_WS_FILTER_IMPL_SFX = ".ws.filter.impl";

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
  public static final String GRILL_SERVER_SNAPSHOT_INTERVAL = "grill.server.snapshot.interval";
  public static final long DEFAULT_GRILL_SERVER_SNAPSHOT_INTERVAL = 5 * 60 * 1000;


  // Email related configurations
  public static final String GRILL_WHETHER_MAIL_NOTIFY = "grill.whether.mail.notify";
  public static final String GRILL_WHETHER_MAIL_NOTIFY_DEFAULT = "false";
  public static final String GRILL_MAIL_FROM_ADDRESS = "grill.mail.from.address";
  public static final String GRILL_MAIL_HOST = "grill.mail.host";
  public static final String GRILL_MAIL_PORT = "grill.mail.port";
  public static final String GRILL_MAIL_SMTP_TIMEOUT = "grill.mail.smtp.timeout";
  public static final String GRILL_MAIL_DEFAULT_SMTP_TIMEOUT = "30000";
  public static final String GRILL_MAIL_SMTP_CONNECTIONTIMEOUT = "grill.mail.smtp.connectiontimeout";
  public static final String GRILL_MAIL_DEFAULT_SMTP_CONNECTIONTIMEOUT = "15000";

  // To be provided by user in query's conf
  public static final String GRILL_QUERY_RESULT_EMAIL_CC = "grill.query.result.email.cc";
  public static final String GRILL_QUERY_RESULT_DEFAULT_EMAIL_CC = "";

  // User session related config
  public static final String GRILL_SESSION_CLUSTER_USER = "grill.session.cluster.user";
  public static final String MAPRED_JOB_QUEUE_NAME = "mapred.job.queue.name";
  public static final String GRILL_SESSION_LOGGEDIN_USER = "grill.session.loggedin.user";

  // ldap user to cluster/hdfs accessing user resolver related configs
  public static final String GRILL_SERVER_USER_RESOLVER_TYPE = "grill.server.user.resolver.type";
  public static final String GRILL_SERVER_USER_RESOLVER_FIXED_VALUE = "grill.server.user.resolver.fixed.value";
  public static final String GRILL_SERVER_USER_RESOLVER_PROPERTYBASED_FILENAME = "grill.server.user.resolver.propertybased.filename";
  public static final String GRILL_SERVER_USER_RESOLVER_DB_DRIVER_NAME = "grill.server.user.resolver.db.driver.name";
  public static final String GRILL_SERVER_USER_RESOLVER_DB_JDBC_URL = "grill.server.user.resolver.db.jdbc.url";
  public static final String GRILL_SERVER_USER_RESOLVER_DB_JDBC_USERNAME = "grill.server.user.resolver.db.jdbc.username";
  public static final String GRILL_SERVER_USER_RESOLVER_DB_JDBC_PASSWORD = "grill.server.user.resolver.db.jdbc.password";
  public static final String GRILL_SERVER_USER_RESOLVER_DB_KEYS = "grill.server.user.resolver.db.keys";
  public static final String GRILL_SERVER_USER_RESOLVER_DB_QUERY = "grill.server.user.resolver.db.query";
  public static final String GRILL_SERVER_USER_RESOLVER_CUSTOM_CLASS = "grill.server.user.resolver.custom.class";

  public static String getServiceImplConfKey(String sName) {
    return GRILL_PFX + sName + GRILL_SERVICE_IMPL_SFX;
  }

  public static String getWSResourceImplConfKey(String rName) {
    return GRILL_PFX + rName + GRILL_WS_RESOURCE_IMPL_SFX;
  }

  public static String getWSFeatureImplConfKey(String featureName) {
    return GRILL_PFX + featureName + GRILL_WS_FEATURE_IMPL_SFX;
  }

  public static String getWSListenerImplConfKey(String listenerName) {
    return GRILL_PFX + listenerName + GRILL_WS_LISTENER_IMPL_SFX;
  }

  public static String getWSFilterImplConfKey(String filterName) {
    return GRILL_PFX + filterName + GRILL_WS_FILTER_IMPL_SFX;
  }

  public static final String ENABLE_CONSOLE_METRICS = "grill.enable.console.metrics";
  public static final String ENABLE_GANGLIA_METRICS = "grill.enable.ganglia.metrics";
  public final static String GANGLIA_SERVERNAME = "grill.metrics.ganglia.host";
  public final static String GANGLIA_PORT = "grill.metrics.ganglia.port";
  public final static String REPORTING_PERIOD = "grill.metrics.reporting.period";

  public static final String GRILL_SERVER_MODE = "grill.server.mode";
  public final static String DEFAULT_GRILL_SERVER_MODE = "OPEN";
  public static final String GRILL_SERVER_DOMAIN = "grill.server.domain";

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

  //Statistics Store configuration keys
  public static final String STATS_STORE_CLASS = "grill.statistics.store.class";

  public static final String GRILL_STATISTICS_WAREHOUSE_KEY = "grill.statistics.warehouse.dir";

  public static final String DEFAULT_STATISTICS_WAREHOUSE = "file:///tmp/grill/statistics/warehouse";

  public static final String GRILL_STATISTICS_DATABASE_KEY = "grill.statistics.db";

  public static final String DEFAULT_STATISTICS_DATABASE = "grillstats";

  public static final String GRILL_STATS_ROLLUP_SCAN_RATE = "grill.statistics.log.rollover.interval";

  public static final long DEFAULT_STATS_ROLLUP_SCAN_RATE = 3600000;

  //Query Purge Configuration

  public static final String MAX_NUMBER_OF_FINISHED_QUERY="grill.max.finished.queries";

  public static final int DEFAULT_FINISHED_QUERIES = 100;

  public static final String GRILL_SERVER_DB_DRIVER_NAME = "grill.server.db.driver.name";
  public static final String DEFAULT_SERVER_DB_DRIVER_NAME = "org.hsqldb.jdbcDriver";
  public static final String GRILL_SERVER_DB_JDBC_URL = "grill.server.db.jdbc.url";
  public static final String DEFAULT_SERVER_DB_JDBC_URL = "jdbc:hsqldb:/tmp/grillserver/queries.db";
  public static final String GRILL_SERVER_DB_JDBC_USER = "grill.server.db.jdbc.user";
  public static final String DEFAULT_SERVER_DB_USER = "SA";
  public static final String GRILL_SERVER_DB_JDBC_PASS = "grill.server.db.jdbc.pass";
  public static final String DEFAULT_SERVER_DB_PASS = "";

  public static final String GRILL_SERVICE_PROVIDER_FACTORY = "grill.server.service.provider.factory";
}
