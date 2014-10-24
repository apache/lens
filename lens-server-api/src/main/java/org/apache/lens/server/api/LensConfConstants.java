package org.apache.lens.server.api;

/*
 * #%L
 * Lens API for server and extensions
 * %%
 * Copyright (C) 2014 Apache Software Foundation
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

public class LensConfConstants {

  // config prefixes
  // All the config variables will use one of these prefixes
  public static final String SERVER_PFX = "lens.server.";
  public static final String QUERY_PFX = "lens.query.";
  public static final String SESSION_PFX = "lens.session.";
  public static final String METASTORE_PFX = "lens.metastore.";

  public static final String DRIVER_CLASSES = SERVER_PFX + "drivers";

  public static final String SERVICE_NAMES = SERVER_PFX + "servicenames";
  public static final String WS_RESOURCE_NAMES = SERVER_PFX + "ws.resourcenames";
  public static final String WS_LISTENER_NAMES = SERVER_PFX + "ws.listenernames";
  public static final String WS_FILTER_NAMES = SERVER_PFX + "ws.filternames";
  public static final String WS_FEATURE_NAMES = SERVER_PFX + "ws.featurenames";

  public static final String SERVICE_IMPL_SFX = ".service.impl";
  public static final String WS_RESOURCE_IMPL_SFX = ".ws.resource.impl";
  public static final String WS_FEATURE_IMPL_SFX = ".ws.feature.impl";
  public static final String WS_LISTENER_IMPL_SFX = ".ws.listener.impl";
  public static final String WS_FILTER_IMPL_SFX = ".ws.filter.impl";

  public static final String QUERY_STATE_LOGGER_ENABLED = SERVER_PFX + "query.state.logger.enabled";
  public static final String EVENT_SERVICE_THREAD_POOL_SIZE = SERVER_PFX + "event.service.thread.pool.size";

  public static final String SERVER_BASE_URL = SERVER_PFX + "base.url";
  public static final String DEFAULT_SERVER_BASE_URL = "http://0.0.0.0:9999/lensapi";

  public static final String SERVER_RESTART_ENABLED = SERVER_PFX + "restart.enabled";
  public static final boolean DEFAULT_SERVER_RESTART_ENABLED = true;
  public static final String SERVER_STATE_PERSIST_LOCATION = SERVER_PFX + "persist.location";
  public static final String DEFAULT_SERVER_STATE_PERSIST_LOCATION = "file:///tmp/lensserver";
  public static final String SERVER_RECOVER_ON_RESTART = SERVER_PFX + "recover.onrestart";
  public static final boolean DEFAULT_SERVER_RECOVER_ON_RESTART = true;
  public static final String SESSION_TIMEOUT_SECONDS = SERVER_PFX + "session.timeout.seconds";
  public static final long SESSION_TIMEOUT_SECONDS_DEFAULT = 1440 * 60; // Default is one day
  public static final String SERVER_UI_URI = SERVER_PFX + "ui.base.uri";
  public static final String DEFAULT_SERVER_UI_URI = "http://0.0.0.0:19999/";
  public static final String SERVER_UI_STATIC_DIR = SERVER_PFX + ".ui.static.dir";
  public static final String DEFAULT_SERVER_UI_STATIC_DIR = "webapp/lens-server/static";
  public static final String SERVER_UI_ENABLE_CACHING = SERVER_PFX + "ui.enable.caching";
  public static final boolean DEFAULT_SERVER_UI_ENABLE_CACHING = true;

  public static final String SERVER_SNAPSHOT_INTERVAL = SERVER_PFX + "snapshot.interval";
  public static final long DEFAULT_SERVER_SNAPSHOT_INTERVAL = 5 * 60 * 1000;

  // Email related configurations
  public static final String QUERY_MAIL_NOTIFY = QUERY_PFX + "enable.mail.notify";
  public static final String WHETHER_MAIL_NOTIFY_DEFAULT = "false";
  public static final String MAIL_FROM_ADDRESS = SERVER_PFX + "mail.from.address";
  public static final String MAIL_HOST = SERVER_PFX + "mail.host";
  public static final String MAIL_PORT = SERVER_PFX + "mail.port";
  public static final String MAIL_SMTP_TIMEOUT = SERVER_PFX + "mail.smtp.timeout";
  public static final String MAIL_DEFAULT_SMTP_TIMEOUT = "30000";
  public static final String MAIL_SMTP_CONNECTIONTIMEOUT = SERVER_PFX + "mail.smtp.connectiontimeout";
  public static final String MAIL_DEFAULT_SMTP_CONNECTIONTIMEOUT = "15000";

  // To be provided by user in query's conf
  public static final String QUERY_RESULT_EMAIL_CC = QUERY_PFX + "result.email.cc";
  public static final String QUERY_RESULT_DEFAULT_EMAIL_CC = "";

  // User session related config
  public static final String SESSION_CLUSTER_USER = SESSION_PFX + "cluster.user";
  public static final String MAPRED_JOB_QUEUE_NAME = "mapred.job.queue.name";
  public static final String SESSION_LOGGEDIN_USER = SESSION_PFX + "loggedin.user";

  // ldap user to cluster/hdfs accessing user resolver related configs
  public static final String USER_RESOLVER_TYPE = SERVER_PFX + "user.resolver.type";
  public static final String USER_RESOLVER_FIXED_VALUE = SERVER_PFX + "user.resolver.fixed.value";
  public static final String USER_RESOLVER_PROPERTYBASED_FILENAME = SERVER_PFX + "user.resolver.propertybased.filename";
  public static final String USER_RESOLVER_DB_DRIVER_NAME = SERVER_PFX + "user.resolver.db.driver.name";
  public static final String USER_RESOLVER_DB_JDBC_URL = SERVER_PFX + "user.resolver.db.jdbc.url";
  public static final String USER_RESOLVER_DB_JDBC_USERNAME = SERVER_PFX + "user.resolver.db.jdbc.username";
  public static final String USER_RESOLVER_DB_JDBC_PASSWORD = SERVER_PFX + "user.resolver.db.jdbc.password";
  public static final String USER_RESOLVER_DB_KEYS = SERVER_PFX + "user.resolver.db.keys";
  public static final String USER_RESOLVER_DB_QUERY = SERVER_PFX + "user.resolver.db.query";
  public static final String USER_RESOLVER_CUSTOM_CLASS = SERVER_PFX + "user.resolver.custom.class";
  public static final String USER_RESOLVER_CACHE_EXPIRY = SERVER_PFX + "user.resolver.cache.expiry";
  public static final String USER_RESOLVER_CACHE_MAX_SIZE = SERVER_PFX + "user.resolver.cache.max_size";
  public static final String USER_RESOLVER_LDAP_URL = SERVER_PFX + "user.resolver.ldap.url";
  public static final String USER_RESOLVER_LDAP_INTERMEDIATE_DB_QUERY = SERVER_PFX + "user.resolver.ldap.intermediate.db.query";
  public static final String USER_RESOLVER_LDAP_FIELDS = SERVER_PFX + "user.resolver.ldap.fields";
  public static final String USER_RESOLVER_LDAP_INTERMEDIATE_DB_INSERT_SQL = SERVER_PFX + "user.resolver.ldap.intermediate.db.insert.sql";
  public static final String USER_RESOLVER_LDAP_INTERMEDIATE_DB_DELETE_SQL = SERVER_PFX + "user.resolver.ldap.intermediate.db.delete.sql";;
  public static final String USER_RESOLVER_LDAP_BIND_DN = SERVER_PFX + "user.resolver.ldap.bind.dn";
  public static final String USER_RESOLVER_LDAP_BIND_PASSWORD = SERVER_PFX + "user.resolver.ldap.bind.password";
  public static final String USER_RESOLVER_LDAP_SEARCH_BASE = SERVER_PFX + "user.resolver.ldap.search.base";
  public static final String USER_RESOLVER_LDAP_SEARCH_FILTER = SERVER_PFX + "user.resolver.ldap.search.filter";

  public static final String STORAGE_COST = METASTORE_PFX + "table.storage.cost";

  // These properties should be set in native tables if their underlying DB or table names are different
  public static final String NATIVE_DB_NAME = METASTORE_PFX + "native.db.name";
  public static final String NATIVE_TABLE_NAME = METASTORE_PFX + "native.table.name";

  public static String getServiceImplConfKey(String sName) {
    return SERVER_PFX + sName + SERVICE_IMPL_SFX;
  }

  public static String getWSResourceImplConfKey(String rName) {
    return SERVER_PFX + rName + WS_RESOURCE_IMPL_SFX;
  }

  public static String getWSFeatureImplConfKey(String featureName) {
    return SERVER_PFX + featureName + WS_FEATURE_IMPL_SFX;
  }

  public static String getWSListenerImplConfKey(String listenerName) {
    return SERVER_PFX + listenerName + WS_LISTENER_IMPL_SFX;
  }

  public static String getWSFilterImplConfKey(String filterName) {
    return SERVER_PFX + filterName + WS_FILTER_IMPL_SFX;
  }

  public static final String ENABLE_CONSOLE_METRICS = SERVER_PFX + "enable.console.metrics";
  public static final String ENABLE_GANGLIA_METRICS = SERVER_PFX + "enable.ganglia.metrics";
  public final static String GANGLIA_SERVERNAME = SERVER_PFX + "metrics.ganglia.host";
  public final static String GANGLIA_PORT = SERVER_PFX + "metrics.ganglia.port";
  public final static String REPORTING_PERIOD = SERVER_PFX + "metrics.reporting.period";

  public static final String SERVER_MODE = SERVER_PFX + "mode";
  public final static String DEFAULT_SERVER_MODE = "OPEN";
  public static final String SERVER_DOMAIN = SERVER_PFX + "domain";

  // resultset output options
  public static final String QUERY_PERSISTENT_RESULT_SET = QUERY_PFX + "enable.persistent.resultset";

  public static final boolean DEFAULT_PERSISTENT_RESULT_SET = false;

  public static final String RESULT_SET_PARENT_DIR = QUERY_PFX + "result.parent.dir";

  public static final String RESULT_SET_PARENT_DIR_DEFAULT = "/tmp/lensreports";

  public static final String QUERY_ADD_INSERT_OVEWRITE = QUERY_PFX + "add.insert.overwrite";

  public static final boolean DEFAULT_ADD_INSERT_OVEWRITE = true;

  public static final String QUERY_PERSISTENT_RESULT_INDRIVER = QUERY_PFX + "enable.persistent.resultset.indriver";

  public static final boolean DEFAULT_DRIVER_PERSISTENT_RESULT_SET = true;

  public static final String QUERY_HDFS_OUTPUT_PATH = QUERY_PFX + "hdfs.output.path";

  public static final String DEFAULT_HDFS_OUTPUT_PATH = "hdfsout";

  public static final String QUERY_OUTPUT_DIRECTORY_FORMAT = QUERY_PFX + "result.output.dir.format";

  public static final String QUERY_OUTPUT_SQL_FORMAT = QUERY_PFX + "result.output.sql.format";

  public static final String QUERY_OUTPUT_FORMATTER = QUERY_PFX + "output.formatter";

  public static final String DEFAULT_INMEMORY_OUTPUT_FORMATTER = "org.apache.lens.lib.query.FileSerdeFormatter";

  public static final String DEFAULT_PERSISTENT_OUTPUT_FORMATTER = "org.apache.lens.lib.query.FilePersistentFormatter";

  public static final String QUERY_OUTPUT_SERDE = QUERY_PFX + "result.output.serde";

  public static final String DEFAULT_OUTPUT_SERDE = "org.apache.lens.lib.query.CSVSerde";

  public static final String QUERY_OUTPUT_FILE_EXTN = QUERY_PFX + "output.file.extn";

  public static final String DEFAULT_OUTPUT_FILE_EXTN = ".csv";

  public static final String QUERY_OUTPUT_CHARSET_ENCODING = QUERY_PFX + "output.charset.encoding";

  public static final String DEFAULT_OUTPUT_CHARSET_ENCODING = "UTF-8";

  public static final String QUERY_OUTPUT_ENABLE_COMPRESSION = QUERY_PFX + "output.enable.compression";

  public static final boolean DEFAULT_OUTPUT_ENABLE_COMPRESSION = false;

  public static final String QUERY_OUTPUT_COMPRESSION_CODEC = QUERY_PFX + "output.compression.codec";

  public static final String DEFAULT_OUTPUT_COMPRESSION_CODEC = "org.apache.hadoop.io.compress.GzipCodec";

  public static final String QUERY_OUTPUT_WRITE_HEADER = QUERY_PFX + "output.write.header";

  public static final boolean DEFAULT_OUTPUT_WRITE_HEADER = false;

  public static final String QUERY_OUTPUT_HEADER = QUERY_PFX + "output.header";

  public static final String QUERY_OUTPUT_WRITE_FOOTER = QUERY_PFX + "output.write.footer";

  public static final boolean DEFAULT_OUTPUT_WRITE_FOOTER = false;

  public static final String QUERY_OUTPUT_FOOTER = QUERY_PFX + "output.footer";

  public static final String RESULT_FORMAT_SIZE_THRESHOLD = QUERY_PFX + "result.size.format.threshold";

  public static final long DEFAULT_RESULT_FORMAT_SIZE_THRESHOLD = 10737418240L; //10GB

  public static final String RESULT_SPLIT_INTO_MULTIPLE = QUERY_PFX + "result.split.multiple";

  public static final boolean DEFAULT_RESULT_SPLIT_INTO_MULTIPLE = false;

  public static final String RESULT_SPLIT_MULTIPLE_MAX_ROWS = QUERY_PFX + "result.split.multiple.maxrows";

  public static final long DEFAULT_RESULT_SPLIT_MULTIPLE_MAX_ROWS = 100000;

  public static final String RESULT_FS_READ_URL = QUERY_PFX + "result.fs.read.url";

  public static final String AUX_JARS = SESSION_PFX + "aux.jars";

  //Statistics Store configuration keys
  public static final String STATS_STORE_CLASS = SERVER_PFX + "statistics.store.class";

  public static final String STATISTICS_WAREHOUSE_KEY = SERVER_PFX + "statistics.warehouse.dir";

  public static final String DEFAULT_STATISTICS_WAREHOUSE = "file:///tmp/lens/statistics/warehouse";

  public static final String STATISTICS_DATABASE_KEY = SERVER_PFX + "statistics.db";

  public static final String DEFAULT_STATISTICS_DATABASE = "lensstats";

  public static final String STATS_ROLLUP_SCAN_RATE = SERVER_PFX + "statistics.log.rollover.interval";

  public static final long DEFAULT_STATS_ROLLUP_SCAN_RATE = 3600000;

  //Query Purge Configuration

  public static final String MAX_NUMBER_OF_FINISHED_QUERY= SERVER_PFX + "max.finished.queries";

  public static final int DEFAULT_FINISHED_QUERIES = 100;

  // Server DB configuration
  public static final String SERVER_DB_DRIVER_NAME = SERVER_PFX + "db.driver.name";
  public static final String DEFAULT_SERVER_DB_DRIVER_NAME = "org.hsqldb.jdbcDriver";
  public static final String SERVER_DB_JDBC_URL = SERVER_PFX + "db.jdbc.url";
  public static final String DEFAULT_SERVER_DB_JDBC_URL = "jdbc:hsqldb:/tmp/lensserver/queries.db";
  public static final String SERVER_DB_JDBC_USER = SERVER_PFX + "db.jdbc.user";
  public static final String DEFAULT_SERVER_DB_USER = "SA";
  public static final String SERVER_DB_JDBC_PASS = SERVER_PFX + "db.jdbc.pass";
  public static final String DEFAULT_SERVER_DB_PASS = "";

  public static final String SERVICE_PROVIDER_FACTORY = SERVER_PFX + "service.provider.factory";
}
