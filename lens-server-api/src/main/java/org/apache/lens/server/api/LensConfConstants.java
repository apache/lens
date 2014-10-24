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
package org.apache.lens.server.api;

/**
 * The Class LensConfConstants.
 */
public class LensConfConstants {

  // config prefixes
  // All the config variables will use one of these prefixes
  /** The Constant SERVER_PFX. */
  public static final String SERVER_PFX = "lens.server.";

  /** The Constant QUERY_PFX. */
  public static final String QUERY_PFX = "lens.query.";

  /** The Constant SESSION_PFX. */
  public static final String SESSION_PFX = "lens.session.";

  /** The Constant METASTORE_PFX. */
  public static final String METASTORE_PFX = "lens.metastore.";

  /** The Constant DRIVER_CLASSES. */
  public static final String DRIVER_CLASSES = SERVER_PFX + "drivers";

  /** The Constant SERVICE_NAMES. */
  public static final String SERVICE_NAMES = SERVER_PFX + "servicenames";

  /** The Constant WS_RESOURCE_NAMES. */
  public static final String WS_RESOURCE_NAMES = SERVER_PFX + "ws.resourcenames";

  /** The Constant WS_LISTENER_NAMES. */
  public static final String WS_LISTENER_NAMES = SERVER_PFX + "ws.listenernames";

  /** The Constant WS_FILTER_NAMES. */
  public static final String WS_FILTER_NAMES = SERVER_PFX + "ws.filternames";

  /** The Constant WS_FEATURE_NAMES. */
  public static final String WS_FEATURE_NAMES = SERVER_PFX + "ws.featurenames";

  /** The Constant SERVICE_IMPL_SFX. */
  public static final String SERVICE_IMPL_SFX = ".service.impl";

  /** The Constant WS_RESOURCE_IMPL_SFX. */
  public static final String WS_RESOURCE_IMPL_SFX = ".ws.resource.impl";

  /** The Constant WS_FEATURE_IMPL_SFX. */
  public static final String WS_FEATURE_IMPL_SFX = ".ws.feature.impl";

  /** The Constant WS_LISTENER_IMPL_SFX. */
  public static final String WS_LISTENER_IMPL_SFX = ".ws.listener.impl";

  /** The Constant WS_FILTER_IMPL_SFX. */
  public static final String WS_FILTER_IMPL_SFX = ".ws.filter.impl";

  /** The Constant QUERY_STATE_LOGGER_ENABLED. */
  public static final String QUERY_STATE_LOGGER_ENABLED = SERVER_PFX + "query.state.logger.enabled";

  /** The Constant EVENT_SERVICE_THREAD_POOL_SIZE. */
  public static final String EVENT_SERVICE_THREAD_POOL_SIZE = SERVER_PFX + "event.service.thread.pool.size";

  /** The Constant SERVER_BASE_URL. */
  public static final String SERVER_BASE_URL = SERVER_PFX + "base.url";

  /** The Constant DEFAULT_SERVER_BASE_URL. */
  public static final String DEFAULT_SERVER_BASE_URL = "http://0.0.0.0:9999/lensapi";

  /** The Constant SERVER_RESTART_ENABLED. */
  public static final String SERVER_RESTART_ENABLED = SERVER_PFX + "restart.enabled";

  /** The Constant DEFAULT_SERVER_RESTART_ENABLED. */
  public static final boolean DEFAULT_SERVER_RESTART_ENABLED = true;

  /** The Constant SERVER_STATE_PERSIST_LOCATION. */
  public static final String SERVER_STATE_PERSIST_LOCATION = SERVER_PFX + "persist.location";

  /** The Constant DEFAULT_SERVER_STATE_PERSIST_LOCATION. */
  public static final String DEFAULT_SERVER_STATE_PERSIST_LOCATION = "file:///tmp/lensserver";

  /** The Constant SERVER_RECOVER_ON_RESTART. */
  public static final String SERVER_RECOVER_ON_RESTART = SERVER_PFX + "recover.onrestart";

  /** The Constant DEFAULT_SERVER_RECOVER_ON_RESTART. */
  public static final boolean DEFAULT_SERVER_RECOVER_ON_RESTART = true;

  /** The Constant SESSION_TIMEOUT_SECONDS. */
  public static final String SESSION_TIMEOUT_SECONDS = SERVER_PFX + "session.timeout.seconds";

  /** The Constant SESSION_TIMEOUT_SECONDS_DEFAULT. */
  public static final long SESSION_TIMEOUT_SECONDS_DEFAULT = 1440 * 60; // Default is one day

  /** The Constant SERVER_UI_URI. */
  public static final String SERVER_UI_URI = SERVER_PFX + "ui.base.uri";

  /** The Constant DEFAULT_SERVER_UI_URI. */
  public static final String DEFAULT_SERVER_UI_URI = "http://0.0.0.0:19999/";

  /** The Constant SERVER_UI_STATIC_DIR. */
  public static final String SERVER_UI_STATIC_DIR = SERVER_PFX + ".ui.static.dir";

  /** The Constant DEFAULT_SERVER_UI_STATIC_DIR. */
  public static final String DEFAULT_SERVER_UI_STATIC_DIR = "webapp/lens-server/static";

  /** The Constant SERVER_UI_ENABLE_CACHING. */
  public static final String SERVER_UI_ENABLE_CACHING = SERVER_PFX + "ui.enable.caching";

  /** The Constant DEFAULT_SERVER_UI_ENABLE_CACHING. */
  public static final boolean DEFAULT_SERVER_UI_ENABLE_CACHING = true;

  /** The Constant SERVER_SNAPSHOT_INTERVAL. */
  public static final String SERVER_SNAPSHOT_INTERVAL = SERVER_PFX + "snapshot.interval";

  /** The Constant DEFAULT_SERVER_SNAPSHOT_INTERVAL. */
  public static final long DEFAULT_SERVER_SNAPSHOT_INTERVAL = 5 * 60 * 1000;

  // Email related configurations
  /** The Constant QUERY_MAIL_NOTIFY. */
  public static final String QUERY_MAIL_NOTIFY = QUERY_PFX + "enable.mail.notify";

  /** The Constant WHETHER_MAIL_NOTIFY_DEFAULT. */
  public static final String WHETHER_MAIL_NOTIFY_DEFAULT = "false";

  /** The Constant MAIL_FROM_ADDRESS. */
  public static final String MAIL_FROM_ADDRESS = SERVER_PFX + "mail.from.address";

  /** The Constant MAIL_HOST. */
  public static final String MAIL_HOST = SERVER_PFX + "mail.host";

  /** The Constant MAIL_PORT. */
  public static final String MAIL_PORT = SERVER_PFX + "mail.port";

  /** The Constant MAIL_SMTP_TIMEOUT. */
  public static final String MAIL_SMTP_TIMEOUT = SERVER_PFX + "mail.smtp.timeout";

  /** The Constant MAIL_DEFAULT_SMTP_TIMEOUT. */
  public static final String MAIL_DEFAULT_SMTP_TIMEOUT = "30000";

  /** The Constant MAIL_SMTP_CONNECTIONTIMEOUT. */
  public static final String MAIL_SMTP_CONNECTIONTIMEOUT = SERVER_PFX + "mail.smtp.connectiontimeout";

  /** The Constant MAIL_DEFAULT_SMTP_CONNECTIONTIMEOUT. */
  public static final String MAIL_DEFAULT_SMTP_CONNECTIONTIMEOUT = "15000";

  // To be provided by user in query's conf
  /** The Constant QUERY_RESULT_EMAIL_CC. */
  public static final String QUERY_RESULT_EMAIL_CC = QUERY_PFX + "result.email.cc";

  /** The Constant QUERY_RESULT_DEFAULT_EMAIL_CC. */
  public static final String QUERY_RESULT_DEFAULT_EMAIL_CC = "";

  // User session related config
  /** The Constant SESSION_CLUSTER_USER. */
  public static final String SESSION_CLUSTER_USER = SESSION_PFX + "cluster.user";

  /** The Constant MAPRED_JOB_QUEUE_NAME. */
  public static final String MAPRED_JOB_QUEUE_NAME = "mapred.job.queue.name";

  /** The Constant SESSION_LOGGEDIN_USER. */
  public static final String SESSION_LOGGEDIN_USER = SESSION_PFX + "loggedin.user";

  // ldap user to cluster/hdfs accessing user resolver related configs
  /** The Constant USER_RESOLVER_TYPE. */
  public static final String USER_RESOLVER_TYPE = SERVER_PFX + "user.resolver.type";

  /** The Constant USER_RESOLVER_FIXED_VALUE. */
  public static final String USER_RESOLVER_FIXED_VALUE = SERVER_PFX + "user.resolver.fixed.value";

  /** The Constant USER_RESOLVER_PROPERTYBASED_FILENAME. */
  public static final String USER_RESOLVER_PROPERTYBASED_FILENAME = SERVER_PFX + "user.resolver.propertybased.filename";

  /** The Constant USER_RESOLVER_DB_DRIVER_NAME. */
  public static final String USER_RESOLVER_DB_DRIVER_NAME = SERVER_PFX + "user.resolver.db.driver.name";

  /** The Constant USER_RESOLVER_DB_JDBC_URL. */
  public static final String USER_RESOLVER_DB_JDBC_URL = SERVER_PFX + "user.resolver.db.jdbc.url";

  /** The Constant USER_RESOLVER_DB_JDBC_USERNAME. */
  public static final String USER_RESOLVER_DB_JDBC_USERNAME = SERVER_PFX + "user.resolver.db.jdbc.username";

  /** The Constant USER_RESOLVER_DB_JDBC_PASSWORD. */
  public static final String USER_RESOLVER_DB_JDBC_PASSWORD = SERVER_PFX + "user.resolver.db.jdbc.password";

  /** The Constant USER_RESOLVER_DB_KEYS. */
  public static final String USER_RESOLVER_DB_KEYS = SERVER_PFX + "user.resolver.db.keys";

  /** The Constant USER_RESOLVER_DB_QUERY. */
  public static final String USER_RESOLVER_DB_QUERY = SERVER_PFX + "user.resolver.db.query";

  /** The Constant USER_RESOLVER_CUSTOM_CLASS. */
  public static final String USER_RESOLVER_CUSTOM_CLASS = SERVER_PFX + "user.resolver.custom.class";

  /** The Constant USER_RESOLVER_CACHE_EXPIRY. */
  public static final String USER_RESOLVER_CACHE_EXPIRY = SERVER_PFX + "user.resolver.cache.expiry";

  /** The Constant USER_RESOLVER_CACHE_MAX_SIZE. */
  public static final String USER_RESOLVER_CACHE_MAX_SIZE = SERVER_PFX + "user.resolver.cache.max_size";

  /** The Constant USER_RESOLVER_LDAP_URL. */
  public static final String USER_RESOLVER_LDAP_URL = SERVER_PFX + "user.resolver.ldap.url";

  /** The Constant USER_RESOLVER_LDAP_INTERMEDIATE_DB_QUERY. */
  public static final String USER_RESOLVER_LDAP_INTERMEDIATE_DB_QUERY = SERVER_PFX
      + "user.resolver.ldap.intermediate.db.query";

  /** The Constant USER_RESOLVER_LDAP_FIELDS. */
  public static final String USER_RESOLVER_LDAP_FIELDS = SERVER_PFX + "user.resolver.ldap.fields";

  /** The Constant USER_RESOLVER_LDAP_INTERMEDIATE_DB_INSERT_SQL. */
  public static final String USER_RESOLVER_LDAP_INTERMEDIATE_DB_INSERT_SQL = SERVER_PFX
      + "user.resolver.ldap.intermediate.db.insert.sql";

  /** The Constant USER_RESOLVER_LDAP_INTERMEDIATE_DB_DELETE_SQL. */
  public static final String USER_RESOLVER_LDAP_INTERMEDIATE_DB_DELETE_SQL = SERVER_PFX
      + "user.resolver.ldap.intermediate.db.delete.sql";;

  /** The Constant USER_RESOLVER_LDAP_BIND_DN. */
  public static final String USER_RESOLVER_LDAP_BIND_DN = SERVER_PFX + "user.resolver.ldap.bind.dn";

  /** The Constant USER_RESOLVER_LDAP_BIND_PASSWORD. */
  public static final String USER_RESOLVER_LDAP_BIND_PASSWORD = SERVER_PFX + "user.resolver.ldap.bind.password";

  /** The Constant USER_RESOLVER_LDAP_SEARCH_BASE. */
  public static final String USER_RESOLVER_LDAP_SEARCH_BASE = SERVER_PFX + "user.resolver.ldap.search.base";

  /** The Constant USER_RESOLVER_LDAP_SEARCH_FILTER. */
  public static final String USER_RESOLVER_LDAP_SEARCH_FILTER = SERVER_PFX + "user.resolver.ldap.search.filter";

  /** The Constant STORAGE_COST. */
  public static final String STORAGE_COST = METASTORE_PFX + "table.storage.cost";

  // These properties should be set in native tables if their underlying DB or table names are different
  /** The Constant NATIVE_DB_NAME. */
  public static final String NATIVE_DB_NAME = METASTORE_PFX + "native.db.name";

  /** The Constant NATIVE_TABLE_NAME. */
  public static final String NATIVE_TABLE_NAME = METASTORE_PFX + "native.table.name";

  /**
   * Gets the service impl conf key.
   *
   * @param sName
   *          the s name
   * @return the service impl conf key
   */
  public static String getServiceImplConfKey(String sName) {
    return SERVER_PFX + sName + SERVICE_IMPL_SFX;
  }

  /**
   * Gets the WS resource impl conf key.
   *
   * @param rName
   *          the r name
   * @return the WS resource impl conf key
   */
  public static String getWSResourceImplConfKey(String rName) {
    return SERVER_PFX + rName + WS_RESOURCE_IMPL_SFX;
  }

  /**
   * Gets the WS feature impl conf key.
   *
   * @param featureName
   *          the feature name
   * @return the WS feature impl conf key
   */
  public static String getWSFeatureImplConfKey(String featureName) {
    return SERVER_PFX + featureName + WS_FEATURE_IMPL_SFX;
  }

  /**
   * Gets the WS listener impl conf key.
   *
   * @param listenerName
   *          the listener name
   * @return the WS listener impl conf key
   */
  public static String getWSListenerImplConfKey(String listenerName) {
    return SERVER_PFX + listenerName + WS_LISTENER_IMPL_SFX;
  }

  /**
   * Gets the WS filter impl conf key.
   *
   * @param filterName
   *          the filter name
   * @return the WS filter impl conf key
   */
  public static String getWSFilterImplConfKey(String filterName) {
    return SERVER_PFX + filterName + WS_FILTER_IMPL_SFX;
  }

  /** The Constant ENABLE_CONSOLE_METRICS. */
  public static final String ENABLE_CONSOLE_METRICS = SERVER_PFX + "enable.console.metrics";

  /** The Constant ENABLE_GANGLIA_METRICS. */
  public static final String ENABLE_GANGLIA_METRICS = SERVER_PFX + "enable.ganglia.metrics";

  /** The Constant GANGLIA_SERVERNAME. */
  public final static String GANGLIA_SERVERNAME = SERVER_PFX + "metrics.ganglia.host";

  /** The Constant GANGLIA_PORT. */
  public final static String GANGLIA_PORT = SERVER_PFX + "metrics.ganglia.port";

  /** The Constant REPORTING_PERIOD. */
  public final static String REPORTING_PERIOD = SERVER_PFX + "metrics.reporting.period";

  /** The Constant SERVER_MODE. */
  public static final String SERVER_MODE = SERVER_PFX + "mode";

  /** The Constant DEFAULT_SERVER_MODE. */
  public final static String DEFAULT_SERVER_MODE = "OPEN";

  /** The Constant SERVER_DOMAIN. */
  public static final String SERVER_DOMAIN = SERVER_PFX + "domain";

  // resultset output options
  /** The Constant QUERY_PERSISTENT_RESULT_SET. */
  public static final String QUERY_PERSISTENT_RESULT_SET = QUERY_PFX + "enable.persistent.resultset";

  /** The Constant DEFAULT_PERSISTENT_RESULT_SET. */
  public static final boolean DEFAULT_PERSISTENT_RESULT_SET = false;

  /** The Constant RESULT_SET_PARENT_DIR. */
  public static final String RESULT_SET_PARENT_DIR = QUERY_PFX + "result.parent.dir";

  /** The Constant RESULT_SET_PARENT_DIR_DEFAULT. */
  public static final String RESULT_SET_PARENT_DIR_DEFAULT = "/tmp/lensreports";

  /** The Constant QUERY_ADD_INSERT_OVEWRITE. */
  public static final String QUERY_ADD_INSERT_OVEWRITE = QUERY_PFX + "add.insert.overwrite";

  /** The Constant DEFAULT_ADD_INSERT_OVEWRITE. */
  public static final boolean DEFAULT_ADD_INSERT_OVEWRITE = true;

  /** The Constant QUERY_PERSISTENT_RESULT_INDRIVER. */
  public static final String QUERY_PERSISTENT_RESULT_INDRIVER = QUERY_PFX + "enable.persistent.resultset.indriver";

  /** The Constant DEFAULT_DRIVER_PERSISTENT_RESULT_SET. */
  public static final boolean DEFAULT_DRIVER_PERSISTENT_RESULT_SET = true;

  /** The Constant QUERY_HDFS_OUTPUT_PATH. */
  public static final String QUERY_HDFS_OUTPUT_PATH = QUERY_PFX + "hdfs.output.path";

  /** The Constant DEFAULT_HDFS_OUTPUT_PATH. */
  public static final String DEFAULT_HDFS_OUTPUT_PATH = "hdfsout";

  /** The Constant QUERY_OUTPUT_DIRECTORY_FORMAT. */
  public static final String QUERY_OUTPUT_DIRECTORY_FORMAT = QUERY_PFX + "result.output.dir.format";

  /** The Constant QUERY_OUTPUT_SQL_FORMAT. */
  public static final String QUERY_OUTPUT_SQL_FORMAT = QUERY_PFX + "result.output.sql.format";

  /** The Constant QUERY_OUTPUT_FORMATTER. */
  public static final String QUERY_OUTPUT_FORMATTER = QUERY_PFX + "output.formatter";

  /** The Constant DEFAULT_INMEMORY_OUTPUT_FORMATTER. */
  public static final String DEFAULT_INMEMORY_OUTPUT_FORMATTER = "org.apache.lens.lib.query.FileSerdeFormatter";

  /** The Constant DEFAULT_PERSISTENT_OUTPUT_FORMATTER. */
  public static final String DEFAULT_PERSISTENT_OUTPUT_FORMATTER = "org.apache.lens.lib.query.FilePersistentFormatter";

  /** The Constant QUERY_OUTPUT_SERDE. */
  public static final String QUERY_OUTPUT_SERDE = QUERY_PFX + "result.output.serde";

  /** The Constant DEFAULT_OUTPUT_SERDE. */
  public static final String DEFAULT_OUTPUT_SERDE = "org.apache.lens.lib.query.CSVSerde";

  /** The Constant QUERY_OUTPUT_FILE_EXTN. */
  public static final String QUERY_OUTPUT_FILE_EXTN = QUERY_PFX + "output.file.extn";

  /** The Constant DEFAULT_OUTPUT_FILE_EXTN. */
  public static final String DEFAULT_OUTPUT_FILE_EXTN = ".csv";

  /** The Constant QUERY_OUTPUT_CHARSET_ENCODING. */
  public static final String QUERY_OUTPUT_CHARSET_ENCODING = QUERY_PFX + "output.charset.encoding";

  /** The Constant DEFAULT_OUTPUT_CHARSET_ENCODING. */
  public static final String DEFAULT_OUTPUT_CHARSET_ENCODING = "UTF-8";

  /** The Constant QUERY_OUTPUT_ENABLE_COMPRESSION. */
  public static final String QUERY_OUTPUT_ENABLE_COMPRESSION = QUERY_PFX + "output.enable.compression";

  /** The Constant DEFAULT_OUTPUT_ENABLE_COMPRESSION. */
  public static final boolean DEFAULT_OUTPUT_ENABLE_COMPRESSION = false;

  /** The Constant QUERY_OUTPUT_COMPRESSION_CODEC. */
  public static final String QUERY_OUTPUT_COMPRESSION_CODEC = QUERY_PFX + "output.compression.codec";

  /** The Constant DEFAULT_OUTPUT_COMPRESSION_CODEC. */
  public static final String DEFAULT_OUTPUT_COMPRESSION_CODEC = "org.apache.hadoop.io.compress.GzipCodec";

  /** The Constant QUERY_OUTPUT_WRITE_HEADER. */
  public static final String QUERY_OUTPUT_WRITE_HEADER = QUERY_PFX + "output.write.header";

  /** The Constant DEFAULT_OUTPUT_WRITE_HEADER. */
  public static final boolean DEFAULT_OUTPUT_WRITE_HEADER = false;

  /** The Constant QUERY_OUTPUT_HEADER. */
  public static final String QUERY_OUTPUT_HEADER = QUERY_PFX + "output.header";

  /** The Constant QUERY_OUTPUT_WRITE_FOOTER. */
  public static final String QUERY_OUTPUT_WRITE_FOOTER = QUERY_PFX + "output.write.footer";

  /** The Constant DEFAULT_OUTPUT_WRITE_FOOTER. */
  public static final boolean DEFAULT_OUTPUT_WRITE_FOOTER = false;

  /** The Constant QUERY_OUTPUT_FOOTER. */
  public static final String QUERY_OUTPUT_FOOTER = QUERY_PFX + "output.footer";

  /** The Constant RESULT_FORMAT_SIZE_THRESHOLD. */
  public static final String RESULT_FORMAT_SIZE_THRESHOLD = QUERY_PFX + "result.size.format.threshold";

  /** The Constant DEFAULT_RESULT_FORMAT_SIZE_THRESHOLD. */
  public static final long DEFAULT_RESULT_FORMAT_SIZE_THRESHOLD = 10737418240L; // 10GB

  /** The Constant RESULT_SPLIT_INTO_MULTIPLE. */
  public static final String RESULT_SPLIT_INTO_MULTIPLE = QUERY_PFX + "result.split.multiple";

  /** The Constant DEFAULT_RESULT_SPLIT_INTO_MULTIPLE. */
  public static final boolean DEFAULT_RESULT_SPLIT_INTO_MULTIPLE = false;

  /** The Constant RESULT_SPLIT_MULTIPLE_MAX_ROWS. */
  public static final String RESULT_SPLIT_MULTIPLE_MAX_ROWS = QUERY_PFX + "result.split.multiple.maxrows";

  /** The Constant DEFAULT_RESULT_SPLIT_MULTIPLE_MAX_ROWS. */
  public static final long DEFAULT_RESULT_SPLIT_MULTIPLE_MAX_ROWS = 100000;

  /** The Constant RESULT_FS_READ_URL. */
  public static final String RESULT_FS_READ_URL = QUERY_PFX + "result.fs.read.url";

  /** The Constant AUX_JARS. */
  public static final String AUX_JARS = SESSION_PFX + "aux.jars";

  // Statistics Store configuration keys
  /** The Constant STATS_STORE_CLASS. */
  public static final String STATS_STORE_CLASS = SERVER_PFX + "statistics.store.class";

  /** The Constant STATISTICS_WAREHOUSE_KEY. */
  public static final String STATISTICS_WAREHOUSE_KEY = SERVER_PFX + "statistics.warehouse.dir";

  /** The Constant DEFAULT_STATISTICS_WAREHOUSE. */
  public static final String DEFAULT_STATISTICS_WAREHOUSE = "file:///tmp/lens/statistics/warehouse";

  /** The Constant STATISTICS_DATABASE_KEY. */
  public static final String STATISTICS_DATABASE_KEY = SERVER_PFX + "statistics.db";

  /** The Constant DEFAULT_STATISTICS_DATABASE. */
  public static final String DEFAULT_STATISTICS_DATABASE = "lensstats";

  /** The Constant STATS_ROLLUP_SCAN_RATE. */
  public static final String STATS_ROLLUP_SCAN_RATE = SERVER_PFX + "statistics.log.rollover.interval";

  /** The Constant DEFAULT_STATS_ROLLUP_SCAN_RATE. */
  public static final long DEFAULT_STATS_ROLLUP_SCAN_RATE = 3600000;

  // Query Purge Configuration

  /** The Constant MAX_NUMBER_OF_FINISHED_QUERY. */
  public static final String MAX_NUMBER_OF_FINISHED_QUERY = SERVER_PFX + "max.finished.queries";

  /** The Constant DEFAULT_FINISHED_QUERIES. */
  public static final int DEFAULT_FINISHED_QUERIES = 100;

  // Server DB configuration
  /** The Constant SERVER_DB_DRIVER_NAME. */
  public static final String SERVER_DB_DRIVER_NAME = SERVER_PFX + "db.driver.name";

  /** The Constant DEFAULT_SERVER_DB_DRIVER_NAME. */
  public static final String DEFAULT_SERVER_DB_DRIVER_NAME = "org.hsqldb.jdbcDriver";

  /** The Constant SERVER_DB_JDBC_URL. */
  public static final String SERVER_DB_JDBC_URL = SERVER_PFX + "db.jdbc.url";

  /** The Constant DEFAULT_SERVER_DB_JDBC_URL. */
  public static final String DEFAULT_SERVER_DB_JDBC_URL = "jdbc:hsqldb:/tmp/lensserver/queries.db";

  /** The Constant SERVER_DB_JDBC_USER. */
  public static final String SERVER_DB_JDBC_USER = SERVER_PFX + "db.jdbc.user";

  /** The Constant DEFAULT_SERVER_DB_USER. */
  public static final String DEFAULT_SERVER_DB_USER = "SA";

  /** The Constant SERVER_DB_JDBC_PASS. */
  public static final String SERVER_DB_JDBC_PASS = SERVER_PFX + "db.jdbc.pass";

  /** The Constant DEFAULT_SERVER_DB_PASS. */
  public static final String DEFAULT_SERVER_DB_PASS = "";

  /** The Constant SERVICE_PROVIDER_FACTORY. */
  public static final String SERVICE_PROVIDER_FACTORY = SERVER_PFX + "service.provider.factory";
}
