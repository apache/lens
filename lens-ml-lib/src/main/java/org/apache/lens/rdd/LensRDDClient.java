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
package org.apache.lens.rdd;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.lens.api.query.*;
import org.apache.lens.client.LensClient;
import org.apache.lens.client.LensClientResultSet;
import org.apache.lens.client.exceptions.LensAPIException;
import org.apache.lens.ml.algo.spark.HiveTableRDD;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 * Create RDD from a Lens query. User can poll returned query handle with isReadyForRDD() until the RDD is ready to be
 * used.
 * </p>
 * Example -
 * <pre>
 *   LensRDDClient client = new LensRDDClient(javaSparkContext);
 *   QueryHandle query = client.createLensRDDAsync("SELECT msr1 from TEST_CUBE WHERE ...", conf);
 *
 *   while (!client.isReadyForRDD(query)) {
 *     Thread.sleep(1000);
 *   }
 *
 *   JavaRDD&lt;ResultRow&gt; rdd = client.getRDD(query).toJavaRDD();
 *
 *   // Consume RDD here -
 *   rdd.map(...);
 * </pre>
 * <p>
 * Alternatively in blocking mode
 * </p>
 * <pre>
 * JavaRDD&lt;ResultRow&gt; rdd = client.createLensRDD(&quot;SELECT msr1 from TEST_CUBE WHERE ...&quot;, conf);
 * </pre>
 */
@Slf4j
public class LensRDDClient {

  // Default input format for table created from Lens result set
  /** The Constant INPUT_FORMAT. */
  private static final String INPUT_FORMAT = TextInputFormat.class.getName();
  // Default output format
  /** The Constant OUTPUT_FORMAT. */
  private static final String OUTPUT_FORMAT = TextOutputFormat.class.getName();
  // Name of partition column and its value. There is always exactly one partition in the table created from
  // Result set.
  /** The Constant TEMP_TABLE_PART_COL. */
  private static final String TEMP_TABLE_PART_COL = "dummy_partition_column";

  /** The Constant TEMP_TABLE_PART_VAL. */
  private static final String TEMP_TABLE_PART_VAL = "placeholder_value";

  /** The Constant HIVE_CONF. */
  protected static final HiveConf HIVE_CONF = new HiveConf();

  static {
    HIVE_CONF.setVar(HiveConf.ConfVars.METASTOREURIS, "");
    HIVE_CONF.set("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=./metastore_db;create=true");
    HIVE_CONF.set("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver");
    HIVE_CONF.setBoolean("hive.metastore.local", true);
    HIVE_CONF.set("hive.metastore.warehouse.dir", "file://${user.dir}/warehouse");
  }

  /** The spark context. */
  private final JavaSparkContext sparkContext; // Spark context

  /** The lens client. */
  private LensClient lensClient; // Lens client instance. Initialized lazily.

  /**
   * Create an RDD client with given spark Context.
   *
   * @param sparkContext the spark context
   */
  public LensRDDClient(JavaSparkContext sparkContext) {
    this.sparkContext = sparkContext;
  }

  /**
   * Create an RDD client with given spark Context.
   *
   * @param sc the sc
   */
  public LensRDDClient(SparkContext sc) {
    this(new JavaSparkContext(sc));
  }

  /**
   * Instantiates a new lens rdd client.
   *
   * @param sparkContext the spark context
   * @param lensClient   the lens client
   */
  public LensRDDClient(JavaSparkContext sparkContext, LensClient lensClient) {
    this.sparkContext = sparkContext;
    this.lensClient = lensClient;
  }

  /**
   * Instantiates a new lens rdd client.
   *
   * @param sparkContext the spark context
   * @param lensClient   the lens client
   */
  public LensRDDClient(SparkContext sparkContext, LensClient lensClient) {
    this(new JavaSparkContext(sparkContext), lensClient);
  }

  private synchronized LensClient getClient() {
    if (lensClient == null) {
      lensClient = new LensClient();
    }
    return lensClient;
  }

  /**
   * API for non blocking use.
   *
   * @param query the query
   * @return the query handle
   * @throws LensAPIException the lens exception
   */
  public QueryHandle createLensRDDAsync(String query) throws LensAPIException {
    return getClient().executeQueryAsynch(query, "");
  }

  /**
   * Check if the RDD is created. RDD will be created as soon as the underlying Lens query is complete
   *
   * @param queryHandle the query handle
   * @return true, if is ready for rdd
   * @throws LensException the lens exception
   */
  public boolean isReadyForRDD(QueryHandle queryHandle) throws LensException {
    QueryStatus status = getClient().getQueryStatus(queryHandle);
    return status.finished();
  }

  /**
   * Allow cancelling underlying query in case of non blocking RDD creation.
   *
   * @param queryHandle the query handle
   * @throws LensException the lens exception
   */
  public void cancelRDD(QueryHandle queryHandle) throws LensException {
    getClient().killQuery(queryHandle);
  }

  /**
   * Get the RDD created for the query. This should be used only is isReadyForRDD returns true
   *
   * @param queryHandle the query handle
   * @return the rdd
   * @throws LensException the lens exception
   */
  public LensRDDResult getRDD(QueryHandle queryHandle) throws LensException {
    QueryStatus status = getClient().getQueryStatus(queryHandle);
    if (!status.finished() && !status.isResultSetAvailable()) {
      throw new LensException(queryHandle.getHandleId() + " query not finished or result unavailable");
    }

    LensClient.LensClientResultSetWithStats result = getClient().getAsyncResults(queryHandle);

    if (result.getResultSet() == null) {
      throw new LensException("Result set not available for query " + queryHandle.getHandleId());
    }

    LensClientResultSet resultSet = result.getResultSet();
    QueryResultSetMetadata metadata = result.getResultSet().getResultSetMetadata();

    // TODO allow creating RDD from in-memory result sets
    if (!(resultSet.getResult() instanceof PersistentQueryResult)) {
      throw new LensException("RDDs only supported for persistent result sets");
    }

    PersistentQueryResult persistentQueryResult = (PersistentQueryResult) resultSet.getResult();

    String tempTableName;
    try {
      tempTableName = createTempMetastoreTable(persistentQueryResult.getPersistedURI(), metadata);
    } catch (HiveException e) {
      throw new LensException("Error creating temp table from result set", e);
    }

    // Now create one RDD
    JavaPairRDD<WritableComparable, HCatRecord> rdd = null;
    try {
      rdd = HiveTableRDD.createHiveTableRDD(sparkContext, HIVE_CONF, "default", tempTableName, TEMP_TABLE_PART_COL
        + "='" + TEMP_TABLE_PART_VAL + "'");
      log.info("Created RDD {} for table {}", rdd.name(), tempTableName);
    } catch (IOException e) {
      throw new LensException("Error creating RDD for table " + tempTableName, e);
    }

    return new LensRDDResult(rdd.map(new HCatRecordToObjectListMapper()).rdd(), queryHandle, tempTableName);
  }

  // Create a temp table with schema of the result set and location

  /**
   * Creates the temp metastore table.
   *
   * @param dataLocation the data location
   * @param metadata     the metadata
   * @return the string
   * @throws HiveException the hive exception
   */
  protected String createTempMetastoreTable(String dataLocation, QueryResultSetMetadata metadata) throws HiveException {
    String tableName = "lens_rdd_" + UUID.randomUUID().toString().replace("-", "_");

    Hive hiveClient = Hive.get(HIVE_CONF);
    Table tbl = hiveClient.newTable("default." + tableName);
    tbl.setTableType(TableType.MANAGED_TABLE);
    tbl.setInputFormatClass(INPUT_FORMAT);
    // String outputFormat = null;
    // tbl.setOutputFormatClass(outputFormat);

    // Add columns
    for (ResultColumn rc : metadata.getColumns()) {
      tbl.getCols().add(new FieldSchema(rc.getName(), toHiveType(rc.getType()), "default"));
      System.out.println("@@@@ COL " + rc.getName() + " TYPE " + toHiveType(rc.getType()));
    }

    tbl.getPartCols().add(new FieldSchema(TEMP_TABLE_PART_COL, "string", "default"));
    hiveClient.createTable(tbl);

    log.info("Table {} created", tableName);

    // Add partition to the table
    AddPartitionDesc partitionDesc = new AddPartitionDesc("default", tableName, false);
    Map<String, String> partSpec = new HashMap<String, String>();
    partSpec.put(TEMP_TABLE_PART_COL, TEMP_TABLE_PART_VAL);
    partitionDesc.addPartition(partSpec, dataLocation);
    hiveClient.createPartitions(partitionDesc);
    log.info("Created partition in {} for data in {}", tableName, dataLocation);

    return tableName;
  }

  // Convert lens data type to Hive data type.

  /**
   * To hive type.
   *
   * @param type the type
   * @return the string
   */
  private String toHiveType(ResultColumnType type) {
    return type.name().toLowerCase();
  }

  /**
   * Blocking call to create an RDD from a Lens query. Return only when the query is complete.
   *
   * @param query the query
   * @return the lens rdd result
   * @throws LensException the lens exception
   */
  public LensRDDResult createLensRDD(String query) throws LensAPIException, LensException {
    QueryHandle queryHandle = createLensRDDAsync(query);
    while (!isReadyForRDD(queryHandle)) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        log.warn("Interrupted while waiting for query", e);
        break;
      }
    }
    return getRDD(queryHandle);
  }

  /**
   * Container object to store the RDD and corresponding Lens query handle.
   */
  public static class LensRDDResult implements Serializable {

    /** The result rdd. */
    private transient RDD<List<Object>> resultRDD;

    /** The lens query. */
    private QueryHandle lensQuery;

    /** The temp table name. */
    private String tempTableName;

    /**
     * Instantiates a new lens rdd result.
     *
     * @param rdd           the rdd
     * @param lensQuery     the lens query
     * @param tempTableName the temp table name
     */
    public LensRDDResult(RDD<List<Object>> rdd, QueryHandle lensQuery, String tempTableName) {
      this.resultRDD = rdd;
      this.lensQuery = lensQuery;
      this.tempTableName = tempTableName;
    }

    /**
     * Instantiates a new lens rdd result.
     */
    public LensRDDResult() {

    }

    public QueryHandle getLensQuery() {
      return lensQuery;
    }

    public RDD<List<Object>> getRDD() {
      return resultRDD;
    }

    /**
     * Recreate RDD. This will work if the result object was saved. As long as the metastore and corresponding HDFS
     * directory is available result object should be able to recreate an RDD.
     *
     * @param sparkContext the spark context
     * @return the rdd
     * @throws LensException the lens exception
     */
    public RDD<List<Object>> recreateRDD(JavaSparkContext sparkContext) throws LensException {
      if (resultRDD == null) {
        try {
          JavaPairRDD<WritableComparable, HCatRecord> javaPairRDD = HiveTableRDD.createHiveTableRDD(sparkContext,
            HIVE_CONF, "default", tempTableName, TEMP_TABLE_PART_COL + "='" + TEMP_TABLE_PART_VAL + "'");
          resultRDD = javaPairRDD.map(new HCatRecordToObjectListMapper()).rdd();
          log.info("Created RDD {} for table {}", resultRDD.name(), tempTableName);
        } catch (IOException e) {
          throw new LensException("Error creating RDD for table " + tempTableName, e);
        }
      }
      return resultRDD;
    }

    public String getTempTableName() {
      return tempTableName;
    }

    /**
     * Delete temp table. This should be done to release underlying temp table.
     *
     * @throws LensException the lens exception
     */
    public void deleteTempTable() throws LensException {
      Hive hiveClient = null;
      try {
        hiveClient = Hive.get(HIVE_CONF);
        hiveClient.dropTable("default." + tempTableName);
        log.info("Dropped temp table {}", tempTableName);
      } catch (HiveException e) {
        throw new LensException(e);
      }
    }
  }

}
