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

import javax.ws.rs.core.MediaType;

import org.apache.lens.api.parse.Parser;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.metastore.*;
import org.apache.lens.server.api.query.DefaultDownloadResultUrlProvider;
import org.apache.lens.server.api.query.DownloadResultUrlProvider;
import org.apache.lens.server.api.query.cost.FactPartitionBasedQueryCost;
import org.apache.lens.server.api.query.cost.QueryCost;

/**
 * The Class LensConfConstants.
 */
public final class LensConfConstants {

  private LensConfConstants() throws LensException {
    throw new LensException("Can't instantiate LensConfConstants");
  }

  // config prefixes
  // All the config variables will use one of these prefixes
  /**
   * The Constant SERVER_PFX.
   */
  public static final String SERVER_PFX = "lens.server.";

  /**
   * The Constant QUERY_PFX.
   */
  public static final String QUERY_PFX = "lens.query.";

  /**
   * The Constant SESSION_PFX.
   */
  public static final String SESSION_PFX = "lens.session.";

  /**
   * The Constant METASTORE_PFX.
   */
  public static final String METASTORE_PFX = "lens.metastore.";

  /**
   * The Constant DRIVER_PFX.
   */
  public static final String DRIVER_PFX = "lens.driver.";

  /**
   * The Constant DRIVER_TYPES_AND_CLASSES
   */
  public static final String DRIVER_TYPES_AND_CLASSES = SERVER_PFX + "drivers";
  /**
   * The Constant DRIVER_SELECTOR_CLASS.
   */
  public static final String DRIVER_SELECTOR_CLASS = SERVER_PFX + "driver.selector.class";
  /**
   * The Constant ACCEPTOR_CLASSES.
   */

  public static final String ACCEPTOR_CLASSES = SERVER_PFX + "query.acceptors";
  /**
   * The Constant SERVICE_NAMES.
   */
  public static final String SERVICE_NAMES = SERVER_PFX + "servicenames";

  /**
   * The Constant WS_RESOURCE_NAMES.
   */
  public static final String WS_RESOURCE_NAMES = SERVER_PFX + "ws.resourcenames";

  /**
   * The Constant WS_LISTENER_NAMES.
   */
  public static final String WS_LISTENER_NAMES = SERVER_PFX + "ws.listenernames";

  /**
   * The Constant WS_FILTER_NAMES.
   */
  public static final String WS_FILTER_NAMES = SERVER_PFX + "ws.filternames";

  /**
   * The Constant WS_FEATURE_NAMES.
   */
  public static final String WS_FEATURE_NAMES = SERVER_PFX + "ws.featurenames";

  public static final String MAX_SESSIONS_PER_USER = SERVER_PFX + "max.sessions.per.user";

  public static final String QUERY_COMPARATOR_CLASSES = SERVER_PFX + "query.comparator.classes";

  public static final Integer DEFAULT_MAX_SESSIONS_PER_USER = 10;

  /**
   * The Constant SERVICE_IMPL_SFX.
   */
  public static final String SERVICE_IMPL_SFX = ".service.impl";

  /**
   * The Constant WS_RESOURCE_IMPL_SFX.
   */
  public static final String WS_RESOURCE_IMPL_SFX = ".ws.resource.impl";

  /**
   * The Constant WS_FEATURE_IMPL_SFX.
   */
  public static final String WS_FEATURE_IMPL_SFX = ".ws.feature.impl";

  /**
   * The Constant WS_LISTENER_IMPL_SFX.
   */
  public static final String WS_LISTENER_IMPL_SFX = ".ws.listener.impl";

  /**
   * The Constant WS_FILTER_IMPL_SFX.
   */
  public static final String WS_FILTER_IMPL_SFX = ".ws.filter.impl";

  /**
   * The Constant QUERY_STATE_LOGGER_ENABLED.
   */
  public static final String QUERY_STATE_LOGGER_ENABLED = SERVER_PFX + "query.state.logger.enabled";

  /**
   * The Constant EVENT_SERVICE_THREAD_POOL_SIZE.
   */
  public static final String EVENT_SERVICE_THREAD_POOL_SIZE = SERVER_PFX + "event.service.thread.pool.size";

  /**
   * The Constant SERVER_BASE_URL.
   */
  public static final String SERVER_BASE_URL = SERVER_PFX + "base.url";

  /**
   * The Constant DEFAULT_SERVER_BASE_URL.
   */
  public static final String DEFAULT_SERVER_BASE_URL = "http://0.0.0.0:9999/lensapi";

  /**
   * The Constant SERVER_STATE_PERSISTENCE_ENABLED.
   */
  public static final String SERVER_STATE_PERSISTENCE_ENABLED = SERVER_PFX + "state.persistence.enabled";

  /**
   * The Constant DEFAULT_SERVER_STATE_PERSISTENCE_ENABLED.
   */
  public static final boolean DEFAULT_SERVER_STATE_PERSISTENCE_ENABLED = true;

  /**
   * If this is false and same query is submitted by a user in the same session
   * and with the same configuration while earlier query is not completed then
   * lens server will return the handle of the previous query.
   */
  public static final String SERVER_DUPLICATE_QUERY_ALLOWED = SERVER_PFX + "duplicate.query.allowed";

  /**
   * By default same query by same user is not allowed.
   */
  public static final boolean DEFAULT_SERVER_DUPLICATE_QUERY_ALLOWED = false;

  /**
   * The Constant SERVER_STATE_PERSIST_LOCATION.
   */
  public static final String SERVER_STATE_PERSIST_LOCATION = SERVER_PFX + "persist.location";

  /**
   * The Constant DEFAULT_SERVER_STATE_PERSIST_LOCATION.
   */
  public static final String DEFAULT_SERVER_STATE_PERSIST_LOCATION = "file:///tmp/lensserver";

  /**
   * The Constant SESSION_TIMEOUT_SECONDS.
   */
  public static final String SESSION_TIMEOUT_SECONDS = SERVER_PFX + "session.timeout.seconds";

  /**
   * The Constant SESSION_TIMEOUT_SECONDS_DEFAULT.
   */
  public static final long SESSION_TIMEOUT_SECONDS_DEFAULT = 1440 * 60; // Default is one day

  /**
   * The Constant SERVER_STATE_PERSISTENCE_INTERVAL_MILLIS.
   */
  public static final String SERVER_STATE_PERSISTENCE_INTERVAL_MILLIS =
    SERVER_PFX + "state.persistence.interval.millis";

  /**
   * The Constant DEFAULT_SERVER_STATE_PERSISTENCE_INTERVAL_MILLIS.
   */
  public static final long DEFAULT_SERVER_STATE_PERSISTENCE_INTERVAL_MILLIS = 5 * 60 * 1000;

  // Email related configurations
  /**
   * The Constant QUERY_MAIL_NOTIFY.
   */
  public static final String QUERY_MAIL_NOTIFY = QUERY_PFX + "enable.mail.notify";

  /**
   * The Constant WHETHER_MAIL_NOTIFY_DEFAULT.
   */
  public static final String WHETHER_MAIL_NOTIFY_DEFAULT = "false";

  /**
   * The Constant MAIL_FROM_ADDRESS.
   */
  public static final String MAIL_FROM_ADDRESS = SERVER_PFX + "mail.from.address";

  /**
   * The Constant MAIL_HOST.
   */
  public static final String MAIL_HOST = SERVER_PFX + "mail.host";

  /**
   * The Constant MAIL_PORT.
   */
  public static final String MAIL_PORT = SERVER_PFX + "mail.port";

  /**
   * The Constant MAIL_SMTP_TIMEOUT.
   */
  public static final String MAIL_SMTP_TIMEOUT = SERVER_PFX + "mail.smtp.timeout";

  /**
   * The Constant MAIL_DEFAULT_SMTP_TIMEOUT.
   */
  public static final String MAIL_DEFAULT_SMTP_TIMEOUT = "30000";

  /**
   * The Constant MAIL_SMTP_CONNECTIONTIMEOUT.
   */
  public static final String MAIL_SMTP_CONNECTIONTIMEOUT = SERVER_PFX + "mail.smtp.connectiontimeout";

  /**
   * The Constant MAIL_DEFAULT_SMTP_CONNECTIONTIMEOUT.
   */
  public static final String MAIL_DEFAULT_SMTP_CONNECTIONTIMEOUT = "15000";

  // To be provided by user in query's conf
  /**
   * The Constant QUERY_RESULT_EMAIL_CC.
   */
  public static final String QUERY_RESULT_EMAIL_CC = QUERY_PFX + "result.email.cc";

  /**
   * The Constant QUERY_RESULT_DEFAULT_EMAIL_CC.
   */
  public static final String QUERY_RESULT_DEFAULT_EMAIL_CC = "";

  // User session related config
  /**
   * The Constant SESSION_CLUSTER_USER.
   */
  public static final String SESSION_CLUSTER_USER = SESSION_PFX + "cluster.user";

  /**
   * The Constant MAPRED_JOB_QUEUE_NAME.
   */
  public static final String MAPRED_JOB_QUEUE_NAME = "mapred.job.queue.name";

  /**
   * The Constant SESSION_LOGGEDIN_USER.
   */
  public static final String SESSION_LOGGEDIN_USER = SESSION_PFX + "loggedin.user";

  /**
   * constant for session proxy user
   */
  public static final String SESSION_PROXY_USER = SESSION_PFX + "proxy.user";

  // ldap user to cluster/hdfs accessing user resolver related configs
  /**
   * The Constant USER_RESOLVER_TYPE.
   */
  public static final String USER_RESOLVER_TYPE = SERVER_PFX + "user.resolver.type";

  /**
   * The Constant USER_RESOLVER_FIXED_VALUE.
   */
  public static final String USER_RESOLVER_FIXED_VALUE = SERVER_PFX + "user.resolver.fixed.value";

  /**
   * The Constant USER_RESOLVER_PROPERTYBASED_FILENAME.
   */
  public static final String USER_RESOLVER_PROPERTYBASED_FILENAME = SERVER_PFX + "user.resolver.propertybased.filename";

  /**
   * The Constant USER_RESOLVER_DB_KEYS.
   */
  public static final String USER_RESOLVER_DB_KEYS = SERVER_PFX + "user.resolver.db.keys";

  /**
   * The Constant USER_RESOLVER_DB_QUERY.
   */
  public static final String USER_RESOLVER_DB_QUERY = SERVER_PFX + "user.resolver.db.query";

  /**
   * The Constant USER_RESOLVER_CUSTOM_CLASS.
   */
  public static final String USER_RESOLVER_CUSTOM_CLASS = SERVER_PFX + "user.resolver.custom.class";

  /**
   * The Constant USER_RESOLVER_CACHE_EXPIRY.
   */
  public static final String USER_RESOLVER_CACHE_EXPIRY = SERVER_PFX + "user.resolver.cache.expiry";

  /**
   * The Constant USER_RESOLVER_CACHE_MAX_SIZE.
   */
  public static final String USER_RESOLVER_CACHE_MAX_SIZE = SERVER_PFX + "user.resolver.cache.max_size";

  /**
   * The Constant USER_RESOLVER_LDAP_URL.
   */
  public static final String USER_RESOLVER_LDAP_URL = SERVER_PFX + "user.resolver.ldap.url";

  /**
   * The Constant USER_RESOLVER_LDAP_INTERMEDIATE_DB_QUERY.
   */
  public static final String USER_RESOLVER_LDAP_INTERMEDIATE_DB_QUERY = SERVER_PFX
    + "user.resolver.ldap.intermediate.db.query";

  /**
   * The Constant USER_RESOLVER_LDAP_FIELDS.
   */
  public static final String USER_RESOLVER_LDAP_FIELDS = SERVER_PFX + "user.resolver.ldap.fields";

  /**
   * The Constant USER_RESOLVER_LDAP_INTERMEDIATE_DB_INSERT_SQL.
   */
  public static final String USER_RESOLVER_LDAP_INTERMEDIATE_DB_INSERT_SQL = SERVER_PFX
    + "user.resolver.ldap.intermediate.db.insert.sql";

  /**
   * The Constant USER_RESOLVER_LDAP_INTERMEDIATE_DB_DELETE_SQL.
   */
  public static final String USER_RESOLVER_LDAP_INTERMEDIATE_DB_DELETE_SQL = SERVER_PFX
    + "user.resolver.ldap.intermediate.db.delete.sql";

  /**
   * The Constant USER_RESOLVER_LDAP_BIND_DN.
   */
  public static final String USER_RESOLVER_LDAP_BIND_DN = SERVER_PFX + "user.resolver.ldap.bind.dn";

  /**
   * The Constant USER_RESOLVER_LDAP_BIND_PASSWORD.
   */
  public static final String USER_RESOLVER_LDAP_BIND_PASSWORD = SERVER_PFX + "user.resolver.ldap.bind.password";

  /**
   * The Constant USER_RESOLVER_LDAP_SEARCH_BASE.
   */
  public static final String USER_RESOLVER_LDAP_SEARCH_BASE = SERVER_PFX + "user.resolver.ldap.search.base";

  /**
   * The Constant USER_RESOLVER_LDAP_SEARCH_FILTER.
   */
  public static final String USER_RESOLVER_LDAP_SEARCH_FILTER = SERVER_PFX + "user.resolver.ldap.search.filter";

  /**
   * The Constant STORAGE_COST.
   */
  public static final String STORAGE_COST = METASTORE_PFX + "table.storage.cost";

  // These properties should be set in native tables if their underlying DB or table names are different
  /**
   * The Constant NATIVE_DB_NAME.
   */
  public static final String NATIVE_DB_NAME = METASTORE_PFX + "native.db.name";

  /**
   * The Constant NATIVE_TABLE_NAME.
   */
  public static final String NATIVE_TABLE_NAME = METASTORE_PFX + "native.table.name";

  /**
   * The property name for setting the column mapping, if column names in native table are different
   */
  public static final String NATIVE_TABLE_COLUMN_MAPPING = METASTORE_PFX + "native.table.column.mapping";

  /**
   * The property name for setting the column mapping, if column names in cubes are different
   */
  public static final String CUBE_COLUMN_MAPPING = METASTORE_PFX + "cube.column.mapping";

  /**
   * The Constant ES_INDEX_NAME.
   */
  public static final String ES_INDEX_NAME = METASTORE_PFX + "es.index.name";

  /**
   * The Constant ES_TYPE_NAME.
   */
  public static final String ES_TYPE_NAME = METASTORE_PFX + "es.type.name";

  /**
   * Gets the service impl conf key.
   *
   * @param sName the s name
   * @return the service impl conf key
   */
  public static String getServiceImplConfKey(String sName) {
    return SERVER_PFX + sName + SERVICE_IMPL_SFX;
  }

  /**
   * Gets the WS resource impl conf key.
   *
   * @param rName the r name
   * @return the WS resource impl conf key
   */
  public static String getWSResourceImplConfKey(String rName) {
    return SERVER_PFX + rName + WS_RESOURCE_IMPL_SFX;
  }

  /**
   * Gets the WS feature impl conf key.
   *
   * @param featureName the feature name
   * @return the WS feature impl conf key
   */
  public static String getWSFeatureImplConfKey(String featureName) {
    return SERVER_PFX + featureName + WS_FEATURE_IMPL_SFX;
  }

  /**
   * Gets the WS listener impl conf key.
   *
   * @param listenerName the listener name
   * @return the WS listener impl conf key
   */
  public static String getWSListenerImplConfKey(String listenerName) {
    return SERVER_PFX + listenerName + WS_LISTENER_IMPL_SFX;
  }

  /**
   * Gets the WS filter impl conf key.
   *
   * @param filterName the filter name
   * @return the WS filter impl conf key
   */
  public static String getWSFilterImplConfKey(String filterName) {
    return SERVER_PFX + filterName + WS_FILTER_IMPL_SFX;
  }

  /**
   * The Constant ENABLE_CONSOLE_METRICS.
   */
  public static final String ENABLE_CONSOLE_METRICS = SERVER_PFX + "enable.console.metrics";

  /** Whether to enable CSV metrics */
  public static final String ENABLE_CSV_METRICS = SERVER_PFX + "enable.csv.metrics";

  /** The directory in which to send CSV metrics */
  public static final String CSV_METRICS_DIRECTORY_PATH = SERVER_PFX + "metrics.csv.directory.path";

  /**
   * The Constant ENABLE_GANGLIA_METRICS.
   */
  public static final String ENABLE_GANGLIA_METRICS = SERVER_PFX + "enable.ganglia.metrics";

  /**
   * The Constant GANGLIA_SERVERNAME.
   */
  public static final String GANGLIA_SERVERNAME = SERVER_PFX + "metrics.ganglia.host";

  /**
   * The Constant GANGLIA_PORT.
   */
  public static final String GANGLIA_PORT = SERVER_PFX + "metrics.ganglia.port";

  /** default ganglia port */
  public static final int DEFAULT_GANGLIA_PORT = 8080;

  /** whether to report metrics to graphite or not */
  public static final String ENABLE_GRAPHITE_METRICS = SERVER_PFX + "enable.graphite.metrics";

  /** graphite server hostname */
  public static final String GRAPHITE_SERVERNAME = SERVER_PFX + "metrics.graphite.host";

  /** graphite server port */
  public static final String GRAPHITE_PORT = SERVER_PFX + "metrics.graphite.port";

  /** default graphite port */
  public static final int DEFAULT_GRAPHITE_PORT = 8080;

  /** whether to enable per resource method metering */
  public static final String ENABLE_RESOURCE_METHOD_METERING = SERVER_PFX + "enable.resource.method.metering";

  /**
   * The Constant REPORTING_PERIOD.
   */
  public static final String REPORTING_PERIOD = SERVER_PFX + "metrics.reporting.period";

  /**
   * The Constant SERVER_MODE.
   */
  public static final String SERVER_MODE = SERVER_PFX + "mode";

  /**
   * The Constant DEFAULT_SERVER_MODE.
   */
  public static final String DEFAULT_SERVER_MODE = "OPEN";

  /**
   * The Constant SERVER_DOMAIN.
   */
  public static final String SERVER_DOMAIN = SERVER_PFX + "domain";

  // resultset output options
  /**
   * The Constant QUERY_PERSISTENT_RESULT_SET.
   */
  public static final String QUERY_PERSISTENT_RESULT_SET = QUERY_PFX + "enable.persistent.resultset";

  /**
   * The Constant DEFAULT_PERSISTENT_RESULT_SET.
   */
  public static final boolean DEFAULT_PERSISTENT_RESULT_SET = false;

  /**
   * The Constant RESULT_SET_PARENT_DIR.
   */
  public static final String RESULT_SET_PARENT_DIR = QUERY_PFX + "result.parent.dir";

  /**
   * The Constant RESULT_SET_PARENT_DIR_DEFAULT.
   */
  public static final String RESULT_SET_PARENT_DIR_DEFAULT = "/tmp/lensreports";

  /**
   * The Constant QUERY_ADD_INSERT_OVEWRITE.
   */
  public static final String QUERY_ADD_INSERT_OVEWRITE = QUERY_PFX + "add.insert.overwrite";

  /**
   * The Constant DEFAULT_ADD_INSERT_OVEWRITE.
   */
  public static final boolean DEFAULT_ADD_INSERT_OVEWRITE = true;

  /**
   * The Constant QUERY_PERSISTENT_RESULT_INDRIVER.
   */
  public static final String QUERY_PERSISTENT_RESULT_INDRIVER = QUERY_PFX + "enable.persistent.resultset.indriver";

  /**
   * The Constant DEFAULT_DRIVER_PERSISTENT_RESULT_SET.
   */
  public static final boolean DEFAULT_DRIVER_PERSISTENT_RESULT_SET = true;

  /**
   * The Constant QUERY_HDFS_OUTPUT_PATH.
   */
  public static final String QUERY_HDFS_OUTPUT_PATH = QUERY_PFX + "hdfs.output.path";

  /**
   * The Constant DEFAULT_HDFS_OUTPUT_PATH.
   */
  public static final String DEFAULT_HDFS_OUTPUT_PATH = "hdfsout";

  /**
   * The Constant QUERY_OUTPUT_DIRECTORY_FORMAT.
   */
  public static final String QUERY_OUTPUT_DIRECTORY_FORMAT = QUERY_PFX + "result.output.dir.format";

  /**
   * The Constant QUERY_OUTPUT_SQL_FORMAT.
   */
  public static final String QUERY_OUTPUT_SQL_FORMAT = QUERY_PFX + "result.output.sql.format";

  /**
   * The Constant QUERY_OUTPUT_FORMATTER.
   */
  public static final String QUERY_OUTPUT_FORMATTER = QUERY_PFX + "output.formatter";

  /**
   * The Constant DEFAULT_INMEMORY_OUTPUT_FORMATTER.
   */
  public static final String DEFAULT_INMEMORY_OUTPUT_FORMATTER = "org.apache.lens.lib.query.FileSerdeFormatter";

  /**
   * The Constant DEFAULT_PERSISTENT_OUTPUT_FORMATTER.
   */
  public static final String DEFAULT_PERSISTENT_OUTPUT_FORMATTER = "org.apache.lens.lib.query.FilePersistentFormatter";

  /**
   * The Constant QUERY_OUTPUT_SERDE.
   */
  public static final String QUERY_OUTPUT_SERDE = QUERY_PFX + "result.output.serde";

  /**
   * The Constant DEFAULT_OUTPUT_SERDE.
   */
  public static final String DEFAULT_OUTPUT_SERDE = "org.apache.lens.lib.query.CSVSerde";

  /**
   * The Constant QUERY_OUTPUT_FILE_EXTN.
   */
  public static final String QUERY_OUTPUT_FILE_EXTN = QUERY_PFX + "output.file.extn";

  /**
   * The Constant DEFAULT_OUTPUT_FILE_EXTN.
   */
  public static final String DEFAULT_OUTPUT_FILE_EXTN = ".csv";

  /**
   * The Constant QUERY_OUTPUT_CHARSET_ENCODING.
   */
  public static final String QUERY_OUTPUT_CHARSET_ENCODING = QUERY_PFX + "output.charset.encoding";

  /**
   * The Constant DEFAULT_OUTPUT_CHARSET_ENCODING.
   */
  public static final String DEFAULT_OUTPUT_CHARSET_ENCODING = "UTF-8";

  /**
   * The Constant QUERY_OUTPUT_ENABLE_COMPRESSION.
   */
  public static final String QUERY_OUTPUT_ENABLE_COMPRESSION = QUERY_PFX + "output.enable.compression";

  /**
   * The Constant DEFAULT_OUTPUT_ENABLE_COMPRESSION.
   */
  public static final boolean DEFAULT_OUTPUT_ENABLE_COMPRESSION = false;

  /**
   * The Constant QUERY_OUTPUT_COMPRESSION_CODEC.
   */
  public static final String QUERY_OUTPUT_COMPRESSION_CODEC = QUERY_PFX + "output.compression.codec";

  /**
   * The Constant DEFAULT_OUTPUT_COMPRESSION_CODEC.
   */
  public static final String DEFAULT_OUTPUT_COMPRESSION_CODEC = "org.apache.hadoop.io.compress.GzipCodec";

  /**
   * The Constant QUERY_OUTPUT_WRITE_HEADER.
   */
  public static final String QUERY_OUTPUT_WRITE_HEADER = QUERY_PFX + "output.write.header";

  /**
   * The Constant DEFAULT_OUTPUT_WRITE_HEADER.
   */
  public static final boolean DEFAULT_OUTPUT_WRITE_HEADER = false;

  /**
   * The Constant QUERY_OUTPUT_HEADER.
   */
  public static final String QUERY_OUTPUT_HEADER = QUERY_PFX + "output.header";

  /**
   * The Constant QUERY_OUTPUT_WRITE_FOOTER.
   */
  public static final String QUERY_OUTPUT_WRITE_FOOTER = QUERY_PFX + "output.write.footer";

  /**
   * The Constant DEFAULT_OUTPUT_WRITE_FOOTER.
   */
  public static final boolean DEFAULT_OUTPUT_WRITE_FOOTER = false;

  /**
   * The Constant QUERY_OUTPUT_FOOTER.
   */
  public static final String QUERY_OUTPUT_FOOTER = QUERY_PFX + "output.footer";

  /**
   * The Constant RESULT_FORMAT_SIZE_THRESHOLD.
   */
  public static final String RESULT_FORMAT_SIZE_THRESHOLD = QUERY_PFX + "result.size.format.threshold";

  /**
   * The Constant DEFAULT_RESULT_FORMAT_SIZE_THRESHOLD.
   */
  public static final long DEFAULT_RESULT_FORMAT_SIZE_THRESHOLD = 10737418240L; // 10GB

  /**
   * The Constant RESULT_SPLIT_INTO_MULTIPLE.
   */
  public static final String RESULT_SPLIT_INTO_MULTIPLE = QUERY_PFX + "result.split.multiple";

  /**
   * The Constant DEFAULT_RESULT_SPLIT_INTO_MULTIPLE.
   */
  public static final boolean DEFAULT_RESULT_SPLIT_INTO_MULTIPLE = false;

  /**
   * The Constant RESULT_SPLIT_MULTIPLE_MAX_ROWS.
   */
  public static final String RESULT_SPLIT_MULTIPLE_MAX_ROWS = QUERY_PFX + "result.split.multiple.maxrows";

  /**
   * The Constant DEFAULT_RESULT_SPLIT_MULTIPLE_MAX_ROWS.
   */
  public static final long DEFAULT_RESULT_SPLIT_MULTIPLE_MAX_ROWS = 100000;

  /**
   * The Constant RESULT_FS_READ_URL.
   */
  public static final String RESULT_FS_READ_URL = QUERY_PFX + "result.fs.read.url";

  /**
   * The Constant AUX_JARS.
   */
  public static final String AUX_JARS = SESSION_PFX + "aux.jars";

  /**
   * Interval at which lens session expiry service runs
   */
  public static final String SESSION_EXPIRY_SERVICE_INTERVAL_IN_SECS = SERVER_PFX
    + "session.expiry.service.interval.secs";

  /**
   * Interval at which KDC login runs
   */
  public static final String KDC_LOGIN_SERVICE_INTERVAL_IN_MINUTES = SERVER_PFX
          + "kdc.login.service.interval.minutes";

  public static final int DEFAULT_SESSION_EXPIRY_SERVICE_INTERVAL_IN_SECS = 3600;

  public static final int DEFAULT_KDC_LOGIN_SERVICE_INTERVAL_IN_MINUTES = 360;

  /**
   * Lens principal for kerberos authentication
   */
  public static final String LENS_PRINCIPAL = SERVER_PFX
          + "principal";


  /**
   * Hive principal for kerberos authentication
   */
  public static final String LENS_DRIVER_HIVE_PRINCIPAL = DRIVER_PFX
          + "hive.kerberos.principal";

  // Statistics Store configuration keys
  /**
   * The Constant STATS_STORE_CLASS.
   */
  public static final String STATS_STORE_CLASS = SERVER_PFX + "statistics.store.class";

  /**
   * The Constant STATISTICS_WAREHOUSE_KEY.
   */
  public static final String STATISTICS_WAREHOUSE_KEY = SERVER_PFX + "statistics.warehouse.dir";

  /**
   * The Constant DEFAULT_STATISTICS_WAREHOUSE.
   */
  public static final String DEFAULT_STATISTICS_WAREHOUSE = "file:///tmp/lens/statistics/warehouse";

  /**
   * The Constant STATISTICS_DATABASE_KEY.
   */
  public static final String STATISTICS_DATABASE_KEY = SERVER_PFX + "statistics.db";

  /**
   * The Constant DEFAULT_STATISTICS_DATABASE.
   */
  public static final String DEFAULT_STATISTICS_DATABASE = "lensstats";

  /**
   * The Constant STATS_ROLLUP_SCAN_RATE.
   */
  public static final String STATS_ROLLUP_SCAN_RATE = SERVER_PFX + "statistics.log.rollover.interval";

  /**
   * The Constant DEFAULT_STATS_ROLLUP_SCAN_RATE.
   */
  public static final long DEFAULT_STATS_ROLLUP_SCAN_RATE = 3600000;

  // Query Purge Configuration

  /**
   * The Constant PURGE_INTERVAL.
   */
  public static final String PURGE_INTERVAL = SERVER_PFX + "querypurger.sleep.interval";

  /**
   * The Constant DEFAULT_PURGE_INTERVAL.
   */
  public static final int DEFAULT_PURGE_INTERVAL = 10000;

  // Server DB configuration
  /**
   * The Constant SERVER_DB_DRIVER_NAME.
   */
  public static final String SERVER_DB_DRIVER_NAME = SERVER_PFX + "db.driver.name";

  /**
   * The Constant DEFAULT_SERVER_DB_DRIVER_NAME.
   */
  public static final String DEFAULT_SERVER_DB_DRIVER_NAME = "org.hsqldb.jdbcDriver";

  /**
   * The Constant SERVER_DB_JDBC_URL.
   */
  public static final String SERVER_DB_JDBC_URL = SERVER_PFX + "db.jdbc.url";

  /**
   * The Constant DEFAULT_SERVER_DB_JDBC_URL.
   */
  public static final String DEFAULT_SERVER_DB_JDBC_URL = "jdbc:hsqldb:/tmp/lensserver/queries.db";

  /**
   * The Constant SERVER_DB_JDBC_USER.
   */
  public static final String SERVER_DB_JDBC_USER = SERVER_PFX + "db.jdbc.user";

  /**
   * The Constant DEFAULT_SERVER_DB_USER.
   */
  public static final String DEFAULT_SERVER_DB_USER = "SA";

  /**
   * The Constant SERVER_DB_JDBC_PASS.
   */
  public static final String SERVER_DB_JDBC_PASS = SERVER_PFX + "db.jdbc.pass";

  /** Validation query to check db pool is valid before passing to the application */
  public static final String SERVER_DB_VALIDATION_QUERY = SERVER_PFX + "db.validation.query";

  /** default value of the validation query */
  public static final String DEFAULT_SERVER_DB_VALIDATION_QUERY = "select 1 from INFORMATION_SCHEMA.SYSTEM_USERS";


  /**
   * The Constant DEFAULT_SERVER_DB_PASS.
   */
  public static final String DEFAULT_SERVER_DB_PASS = "";

  /**
   * The Constant SERVICE_PROVIDER_FACTORY.
   */
  public static final String SERVICE_PROVIDER_FACTORY = SERVER_PFX + "service.provider.factory";

  /**
   * Key for reading Output Stream Buffer Size used in writing lens server state to file system
   */
  public static final String STATE_PERSIST_OUT_STREAM_BUFF_SIZE = SERVER_PFX + "state.persist.out.stream.buffer.size";

  /**
   * Default Output Stream Buffer Size used in writing lens server state to file system: 1MB
   */
  public static final int DEFAULT_STATE_PERSIST_OUT_STREAM_BUFF_SIZE = 1048576;

  /**
   * Key for top level dir of database specific resources
   */
  public static final String DATABASE_RESOURCE_DIR = SERVER_PFX + "database.resource.dir";
  /**
   * Default value of top level dir for database specific resources
   */
  public static final String DEFAULT_DATABASE_RESOURCE_DIR = "/tmp/lens/resources";

  /**
   * Key for enabling metrics for each query to be different
   */
  public static final String ENABLE_QUERY_METRICS = QUERY_PFX + "enable.metrics.per.query";

  /**
   * Default value for query wise metrics
   */
  public static final boolean DEFAULT_ENABLE_QUERY_METRICS = false;

  /**
   * Key used to hold value of unique id for query metrics. This wont be passed by user, will be generated and set.
   * This is to pass unique id for query across the code flow.
   */
  public static final String QUERY_METRIC_UNIQUE_ID_CONF_KEY = QUERY_PFX + "metric.unique.id";

  /**
   * Key used to hold value query metric name in the stack. This wont be passed by user, will be generated and set.
   * When each query looked at by driver, the metric needs to be different for each driver. This name capture the stack
   * from which driver the code reached there.
   */
  public static final String QUERY_METRIC_DRIVER_STACK_NAME = QUERY_PFX + "metric.driver.stack.name";

  /**
   * Timeout for parallel query estimate calls. A driver needs to comeback with a query estimate within this timeout.
   */
  public static final String ESTIMATE_TIMEOUT_MILLIS = SERVER_PFX + "estimate.timeout.millis";

  /**
   * Default value for timeout for parallel estimate calls.
   */
  public static final long DEFAULT_ESTIMATE_TIMEOUT_MILLIS = 300000L; // 5 minutes


  /**
   * Key used to get minimum number of threads in the estimate thread pool
   */
  public static final String ESTIMATE_POOL_MIN_THREADS = SERVER_PFX + "estimate.pool.min.threads";
  public static final int DEFAULT_ESTIMATE_POOL_MIN_THREADS = 3;

  /**
   * Key used to get maximum number of threads in the estimate thread pool
   */
  public static final String ESTIMATE_POOL_MAX_THREADS = SERVER_PFX + "estimate.pool.max.threads";
  public static final int DEFAULT_ESTIMATE_POOL_MAX_THREADS = 100;

  /**
   * Key used to get keep alive time for threads in the estimate thread pool
   */
  public static final String ESTIMATE_POOL_KEEP_ALIVE_MILLIS = SERVER_PFX + "estimate.pool.keepalive.millis";
  public static final int DEFAULT_ESTIMATE_POOL_KEEP_ALIVE_MILLIS = 60000; // 1 minute

  /**
   * Key used to get minimum number of threads in the launcher thread pool
   */
  public static final String LAUNCHER_POOL_MIN_THREADS = SERVER_PFX + "launcher.pool.min.threads";
  public static final int DEFAULT_LAUNCHER_POOL_MIN_THREADS = 3;

  /**
   * Key used to get maximum number of threads in the launcher thread pool
   */
  public static final String LAUNCHER_POOL_MAX_THREADS = SERVER_PFX + "launcher.pool.max.threads";
  // keeping the default to hundred, we may never grow till there, it would go to max for concurrent queries allowed on
  // all drivers together.
  public static final int DEFAULT_LAUNCHER_POOL_MAX_THREADS = 100;

  /**
   * Key used to get keep alive time for threads in the launcher thread pool
   */
  public static final String LAUNCHER_POOL_KEEP_ALIVE_MILLIS = SERVER_PFX + "launcher.pool.keepalive.millis";
  public static final int DEFAULT_LAUNCHER_POOL_KEEP_ALIVE_MILLIS = 60000; // 1 minute

  public static final String QUERY_PHASE1_REWRITERS = SERVER_PFX + "query.phase1.rewriters";

  /**
   * Key to get the implementations of query constraint factories.
   */
  public static final String QUERY_LAUNCHING_CONSTRAINT_FACTORIES_SFX = "query.launching.constraint.factories";
  public static final String QUERY_LAUNCHING_CONSTRAINT_FACTORIES_KEY = SERVER_PFX
    + QUERY_LAUNCHING_CONSTRAINT_FACTORIES_SFX;

  /**
   * Key to get the total query cost ceiling per user.
   */
  public static final String TOTAL_QUERY_COST_CEILING_PER_USER_KEY = SERVER_PFX
      + "total.query.cost.ceiling.per.user";

  /**
   * Key to get the implementations of waiting queries selection policy factories.
   */
  public static final String WAITING_QUERIES_SELECTION_POLICY_FACTORIES_SFX =
    "waiting.queries.selection.policy.factories";
  public static final String WAITING_QUERIES_SELECTION_POLICY_FACTORIES_KEY = SERVER_PFX
      + WAITING_QUERIES_SELECTION_POLICY_FACTORIES_SFX;

  /**
   * Key denoting the dialect class property of saved query service.
   */
  public static final String JDBC_DIALECT_PROVIDER_CLASS_KEY = "lens.server.savedquery.jdbc.dialectclass";

  /**
   * Key denoting the default fetch value of saved query list api.
   */
  public static final String FETCH_COUNT_SAVED_QUERY_LIST_KEY = "lens.server.savedquery.list.default.count";

  /**
   * Default fetch count of saved query list api.
   */
  public static final int DEFAULT_FETCH_COUNT_SAVED_QUERY_LIST = 20;

  /**
   * This is the base directory where all drivers are available under lens-server's Conf directory.
   */
  public static final String DRIVERS_BASE_DIR = "drivers";

  /**
   * The driver weight property
   */
  public static final String DRIVER_WEIGHT = DRIVER_PFX + "weight";

  /**
   * Key for specifying Retry policy class
   */
  public static final String RETRY_POLICY_CLASSES_SFX = "query.retry.policy.classes";

  public static final String QUERY_RETRY_POLICY_CLASSES = SERVER_PFX + RETRY_POLICY_CLASSES_SFX;

  /**
   * Driver hook property
   */
  public static final String DRIVER_HOOK_CLASSES_SFX = "query.hook.classes";

  /**
   * Default driver weight
   */
  public static final int DEFAULT_DRIVER_WEIGHT = 1;

  /**
   * Name of the property that holds the path of "conf" directory of server
   */
  public static final String CONFIG_LOCATION = "config.location";

  /**
   * Default location of "conf" directory (wrt to lens-server/bin)
   */
  public static final String DEFAULT_CONFIG_LOCATION = "../conf";

  /**
   * The Constant RESULTSET_PURGE_ENABLED.
   */
  public static final String RESULTSET_PURGE_ENABLED = SERVER_PFX + "resultset.purge.enabled";

  /**
   * The Constant DEFAULT_RESULTSET_PURGE_ENABLED
   */
  public static final boolean DEFAULT_RESULTSET_PURGE_ENABLED = false;

  /**
   * The Constant RESULTSET_PURGE_INTERVAL_IN_SECONDS.
   */
  public static final String RESULTSET_PURGE_INTERVAL_IN_SECONDS = SERVER_PFX + "resultsetpurger.sleep.interval.secs";

  /*
   * The Constant DEFAULT_RESULTSET_PURGE_INTERVAL_IN_SECONDS.
   */
  public static final int DEFAULT_RESULTSET_PURGE_INTERVAL_IN_SECONDS = 3600;

  /**
   * The Constant QUERY_RESULTSET_RETENTION.
   */
  public static final String QUERY_RESULTSET_RETENTION = SERVER_PFX + "query.resultset.retention";

  /**
   * The Constant DEFAULT_QUERY_RESULTSET_RETENTION.
   */
  public static final String DEFAULT_QUERY_RESULTSET_RETENTION = "1 day";

  /**
   * The Constant HDFS_OUTPUT_RETENTION.
   */
  public static final String HDFS_OUTPUT_RETENTION = SERVER_PFX + "hdfs.output.retention";

  /**
   * The Constant DEFAULT_HDFS_OUTPUT_RETENTION.
   */
  public static final String DEFAULT_HDFS_OUTPUT_RETENTION = "1 day";

  /**
   * Pre Fetch results in case of in memory result sets.
   */
  public static final String PREFETCH_INMEMORY_RESULTSET = QUERY_PFX + "prefetch.inmemory.resultset";

  /**
   * Pre Fetch results in case of in memory result sets is enabled by default
   */
  public static final boolean DEFAULT_PREFETCH_INMEMORY_RESULTSET = true;

  /**
   * Pre-Fetch size for in memory results. Makes sense only if {@link #PREFETCH_INMEMORY_RESULTSET} set to true
   */
  public static final String PREFETCH_INMEMORY_RESULTSET_ROWS = QUERY_PFX + "prefetch.inmemory.resultset.rows";

  /**
   * Default Pre-Fetch size for in memory results.
   */
  public static final int DEFAULT_PREFETCH_INMEMORY_RESULTSET_ROWS = 100;

  /**
   * The Constant EXCLUDE_CUBE_TABLES.
   */
  public static final String EXCLUDE_CUBE_TABLES = SESSION_PFX + "metastore.exclude.cubetables.from.nativetables";

  /**
   * The Constant DEFAULT_EXCLUDE_CUBE_TABLES.
   */
  public static final boolean DEFAULT_EXCLUDE_CUBE_TABLES = true;

  /**
   * This property defines the TTL secs for all result sets of
   * type {@link org.apache.lens.server.api.driver.InMemoryResultSet} beyond which they are eligible for purging
   */
  public static final String INMEMORY_RESULT_SET_TTL_SECS = SERVER_PFX + "inmemory.resultset.ttl.secs";

  /**
   * Default value of  INMEMORY_RESULT_SET_TTL_SECS is 300 secs (5 minutes)
   */
  public static final int DEFAULT_INMEMORY_RESULT_SET_TTL_SECS = 300;

  /**
   * Number of retries status update will be retried, in case of transient failures
   */
  public static final String STATUS_UPDATE_EXPONENTIAL_RETRIES = SERVER_PFX + "status.update.num.retries";

  /**
   * Default value of STATUS_UPDATE_EXPONENTIAL_RETRIES is 10
   */
  public static final int DEFAULT_STATUS_UPDATE_EXPONENTIAL_RETRIES = 10;

  /**
   * Maximum delay a status update can wait for next update, in case of transient failures
   */
  public static final String MAXIMUM_STATUS_UPDATE_DELAY = SERVER_PFX + "status.update.maximum.delay.secs";

  /**
   * Default value of MAXIMUM_STATUS_UPDATE_DELAY is 1800 secs (30 minutes)
   */
  public static final long DEFAULT_MAXIMUM_STATUS_UPDATE_DELAY = 1800;

  /**
   * Number of seconds that would grow exponentially for next update, incase of transient failures.
   */
  public static final String STATUS_UPDATE_EXPONENTIAL_WAIT_FACTOR = SERVER_PFX
    + "status.update.exponential.wait.millis";

  /**
   * Default value of DEFAULT_STATUS_UPDATE_EXPONENTIAL_WAIT_FACTOR is 30000 millis (30 seconds)
   */
  public static final long DEFAULT_STATUS_UPDATE_EXPONENTIAL_WAIT_FACTOR = 30000;

  /**
   * Specifies whether to attempt cancellation of a query whose execution takes longer than the timeout value
   * specified while submitting the query for execution.
   */
  public static final String CANCEL_QUERY_ON_TIMEOUT = QUERY_PFX + "cancel.on.timeout";

  /**
   * Default value of "lens.query.cancel.on.timeout"
   */
  public static final boolean DEFAULT_CANCEL_QUERY_ON_TIMEOUT = true;

  /**
   * Scheduler store class
   */
  public static final java.lang.String SCHEDULER_STORE_CLASS = SERVER_PFX + "scheduler.store.class";

  /**
   * Query current time for the scheduled query.
   */
  public static final String QUERY_CURRENT_TIME_IN_MILLIS = QUERY_PFX + "current.time.millis";

  /**
   * This is the connection timeout for all HTTP Notifications sent by lens server
   */
  public static final String HTTP_NOTIFICATION_CONN_TIMEOUT_MILLIS = SERVER_PFX
    + "http.notification.conn.timeout.millis";

  /**
   * Default connection timeout is 5 secs
   */
  public static final int DEFAULT_HTTP_NOTIFICATION_CONN_TIMEOUT_MILLIS = 5000; //5 secs

  /**
   * This is the read timeout for all HTTP Notifications sent by lens server
   */
  public static final String HTTP_NOTIFICATION_READ_TIMEOUT_MILLIS = SERVER_PFX
    + "http.notification.read.timeout.millis";

  /**
   * Default read timeout is 5 secs
   */
  public static final int DEFAULT_HTTP_NOTIFICATION_READ_TIMEOUT_MILLIS = 10000; //10 secs

  /**
   * This is the media type for Query Http notifications
   */
  public static final String QUERY_HTTP_NOTIFICATION_MEDIATYPE = QUERY_PFX + "http.notification.mediatype";

  /**
   * This is the default media type for all Query Http notifications
   */
  public static final String DEFAULT_QUERY_HTTP_NOTIFICATION_MEDIATYPE = MediaType.APPLICATION_JSON;

  /**
   * These are the end points for Query http notifications. Users can specify more than one comma separated end points
   */
  public static final String QUERY_HTTP_NOTIFICATION_URLS = QUERY_PFX + "http.notification.urls";

  /**
   * This is the prefix for Query http Notification. User is expected to add notification type along with prefix
   */
  public static final String QUERY_HTTP_NOTIFICATION_TYPE_PFX = QUERY_PFX + "http.notification.type.";

  /**
   * This is the property for enabling Query finished Http notification. This will include successful, failed and
   * cancelled queries.
   */
  public static final String QUERY_HTTP_NOTIFICATION_TYPE_FINISHED = QUERY_HTTP_NOTIFICATION_TYPE_PFX + "FINISHED";

  /**
   * This is the property to specify the timeout for a query after running for configured time.
   */
  public static final String QUERY_TIMEOUT_MILLIS = QUERY_PFX + "timeout.millis";

  /**
   * This is the default timeout for query to get killed after running for configured time.
   */
  public static final int DEFAULT_QUERY_TIMEOUT_MILLIS = 24 * 60 * 60 * 1000; // 1day

  /**
   * Specifies how often query expiry will run
   */
  public static final String QUERY_EXPIRY_INTERVAL_MILLIS = SERVER_PFX + "query.expiry.check.interval.millis";
  /**
   * Default value for query expiry interval
   */
  public static final long DEFAULT_QUERY_EXPIRY_INTERVAL_MILLIS = 1 * 60 * 60 * 1000; // 1 hour

  /**
   * The Constant GRIZZLY_CORE_POOL_SIZE.
   */
  public static final String GRIZZLY_CORE_POOL_SIZE = SERVER_PFX + "grizzly.core.pool.size";

  /**
   * The Constant DEFAULT_GRIZZLY_CORE_POOL_SIZE.
   */
  public static final int DEFAULT_GRIZZLY_CORE_POOL_SIZE = 20;

  /**
   * The Constant GRIZZLY_MAX_POOL_SIZE.
   */
  public static final String GRIZZLY_MAX_POOL_SIZE = SERVER_PFX + "grizzly.max.pool.size";

  /**
   * The Constant DEFAULT_GRIZZLY_MAX_POOL_SIZE.
   */
  public static final int DEFAULT_GRIZZLY_MAX_POOL_SIZE = 40;

  /**
   * Thread interval for checking the waiting instances
   */
  public static final String SCHEDULED_INSTANCE_WAITING_THREAD_INTERVAL_MILLIS =
    SERVER_PFX + "scheduler.instance.waiting.thread.interval";

  /**
   * Default waiting thread interval in milliseconds
   */
  public static final long DEFAULT_SCHEDULED_INSTANCE_WAITING_THREAD_INTERVAL_MILLIS = 60 * 5 * 1000;

  /**
   * Default value is less than zero, that means an user can scheduler unlimited number of jobs.
   */
  public static final int DEFAULT_MAX_SCHEDULED_JOB_PER_USER = -1;

  public static final String QUERY_COST_PARSER = SERVER_PFX + "query.cost.parser.class";
  public static final Class<? extends Parser<? extends QueryCost>> DEFAULT_QUERY_COST_PARSER
    = FactPartitionBasedQueryCost.Parser.class;

  /**
   * Maximum number of scheduled job per user.
   */
  public static final String MAX_SCHEDULED_JOB_PER_USER  = SERVER_PFX + "scheduler.max.job.per.user";

  /**
   * The class that implements the DataCompletenessChecker Interface. This will take effect if the flag
   * "lens.cube.metastore.enable.datacompleteness.check" is set.
   */
  public static final String COMPLETENESS_CHECKER_CLASS = "lens.cube.metastore.completeness.checker.class";

  /*The class that implements the result download url provider interface */
  public static final String RESULT_DOWNLOAD_URL_PROVIDER_CLASS = SERVER_PFX + "result.download.url.provider.class";

  /*The  default result download url provider class */
  public static final Class<? extends DownloadResultUrlProvider> DEFAULT_RESULT_DOWNLOAD_URL_PROVIDER =
    DefaultDownloadResultUrlProvider.class.asSubclass(DownloadResultUrlProvider.class);

  /**
   * The download url content config
   */
  public static final String QUERY_EMAIL_DOWNLOAD_URL = QUERY_PFX + "email.download.url";

  /**
   * The default implementation of DataCompletenessChecker
   */
  public static final Class<? extends DataCompletenessChecker> DEFAULT_COMPLETENESS_CHECKER =
          DefaultChecker.class.asSubclass(DataCompletenessChecker.class);

  /**
   * This property is to enable Data Completeness Checks while resolving partitions.
   */
  public static final String ENABLE_DATACOMPLETENESS_CHECK = "lens.cube.metastore.enable.datacompleteness.check";

  /**
   * Default Value of the config "lens.cube.metastore.enable.datacompleteness.check"
   */
  public static final boolean DEFAULT_ENABLE_DATACOMPLETENESS_CHECK = false;

  /**
   * This property is for setting static cost to driver
   */
  public static final String DRIVER_QUERY_COST = DRIVER_PFX + "query.cost";

  /**
   * Config param for defining query type ranges.
   */
  public static final String DRIVER_COST_TYPE_RANGES = "lens.driver.cost.type.ranges";

  /*
  * Default query cost range
  * */
  public static final String DRIVER_QUERY_COST_TYPE_DEFAULT_RANGES = "LOW,0.0,HIGH";

  /**
   * Default Value of the config "lens.driver.query.cost"
   */
  public static final double DEFAULT_DRIVER_QUERY_COST = 0.0;

  public static final String DRIVER_COST_QUERY_DECIDER = DRIVER_PFX + "cost.query.decider.class";

  public static final String AUTH_SCHEME = SERVER_PFX  + "authentication.scheme";

  public static final String KERBEROS_PRINCIPAL = SERVER_PFX + "authentication.kerberos.principal";

  public static final String KERBEROS_REALM = SERVER_PFX + "authentication.kerberos.realm";

  public static final String KERBEROS_KEYTAB = SERVER_PFX + "authentication.kerberos.keytab";

  public static final String ALLOWED_PROXY_USERS = SERVER_PFX + "authentication.allowed.proxy.users";

  public static final String DELEGATION_TOKEN_AUTH_HDFS_PATH_TO_CHECK = SERVER_PFX
    + "delegation.token.auth.hdfs.path.to.check";

  /**
   *  SSL config to enable https communication between lens server
   *  and clients.
   */
  public static final String SSL_ENABLED = SERVER_PFX + "ssl.enabled";

  public static final boolean DEFAULT_SSL_ENABLED_VALUE = false;

  public static final String SSL_KEYSTORE_FILE_PATH = SERVER_PFX + "ssl.file.path";

  public static final String SSL_KEYSTORE_PASSWORD = SERVER_PFX + "ssl.password";

}
