/*
 * #%L
 * Lens ML Lib
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
package org.apache.lens.rdd;

import org.apache.lens.api.LensException;
import org.apache.lens.api.query.*;
import org.apache.lens.client.LensClient;
import org.apache.lens.client.LensClientResultSet;
import org.apache.lens.ml.spark.HiveTableRDD;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * <p>
 * Create RDD from a Lens query. User can poll returned query handle with
 * isReadyForRDD() until the RDD is ready to be used.
 *
 * Example -
 * <pre>
 *   LensRDDClient client = new LensRDDClient(javaSparkContext);
 *   QueryHandle query = client.createLensRDDAsync("SELECT msr1 from TEST_CUBE WHERE ...", conf);
 *
 *   while (!client.isReadyForRDD(query)) {
 *     Thread.sleep(1000);
 *   }
 *
 *   JavaRDD<ResultRow> rdd = client.getRDD(query).toJavaRDD();
 *
 *   // Consume RDD here -
 *   rdd.map(...);
 * </pre>
 *
 * </p>
 *
 * <p>
 * Alternatively in blocking mode
 *
 * <pre>
 *   JavaRDD<ResultRow> rdd = client.createLensRDD("SELECT msr1 from TEST_CUBE WHERE ...", conf);
 * </pre>
 * </p>
 */
public class LensRDDClient {
  public static final Log LOG = LogFactory.getLog(LensRDDClient.class);
  // Default input format for table created from Lens result set
  private static final String INPUT_FORMAT = TextInputFormat.class.getName();
  // Default output format
  private static final String OUTPUT_FORMAT = TextOutputFormat.class.getName();
  // Name of partition column and its value. There is always exactly one partition in the table created from
  // Result set.
  private static final String TEMP_TABLE_PART_COL = "dummy_partition_column";
  private static final String TEMP_TABLE_PART_VAL = "placeholder_value";

  protected static final HiveConf hiveConf = new HiveConf();
  static {
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "");
    hiveConf.set("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=./metastore_db;create=true");
    hiveConf.set("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver");
    hiveConf.setBoolean("hive.metastore.local", true);
    hiveConf.set("hive.metastore.warehouse.dir", "file://${user.dir}/warehouse");
  }

  private final JavaSparkContext sparkContext; // Spark context
  private LensClient lensClient; // Lens client instance. Initialized lazily.

  /**
   * Create an RDD client with given spark Context
   */
  public LensRDDClient(JavaSparkContext sparkContext) {
    this.sparkContext = sparkContext;
  }

  /**
   * Create an RDD client with given spark Context
   */
  public LensRDDClient(SparkContext sc) {
    this(new JavaSparkContext(sc));
  }

  public LensRDDClient(JavaSparkContext sparkContext, LensClient lensClient) {
    this.sparkContext = sparkContext;
    this.lensClient = lensClient;
  }

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
   * API for non blocking use
   **/
  public QueryHandle createLensRDDAsync(String query) throws LensException {
    return getClient().executeQueryAsynch(query, "");
  }

  /**
   * Check if the RDD is created. RDD will be created as soon as the underlying Lens query is complete
   */
  public boolean isReadyForRDD(QueryHandle queryHandle) throws LensException {
    QueryStatus status = getClient().getQueryStatus(queryHandle);
    return status.isFinished();
  }

  /**
   * Allow cancelling underlying query in case of non blocking RDD creation
   */
  public void cancelRDD(QueryHandle queryHandle) throws LensException {
    getClient().killQuery(queryHandle);
  }

  /**
   * Get the RDD created for the query. This should be used only is isReadyForRDD returns true
   */
  public LensRDDResult getRDD(QueryHandle queryHandle) throws LensException {
    QueryStatus status = getClient().getQueryStatus(queryHandle);
    if (!status.isFinished() && !status.isResultSetAvailable()) {
      throw new LensException(queryHandle.getHandleId() + " query not finished or result unavailable");
    }

    LensClient.LensClientResultSetWithStats result = getClient().getAsyncResults(queryHandle);

    if (result.getResultSet() == null) {
      throw new LensException("Result set not available for query " + queryHandle.getHandleId());
    }

    LensClientResultSet resultSet = result.getResultSet();
    QueryResultSetMetadata metadata = result.getResultSet().getResultSetMetadata();

    // TODO allow creating RDD from in-memory result sets
    if (! (resultSet.getResult() instanceof PersistentQueryResult)) {
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
    JavaPairRDD<WritableComparable,HCatRecord> rdd = null;
    try {
      rdd = HiveTableRDD.createHiveTableRDD(sparkContext, hiveConf, "default", tempTableName,
        TEMP_TABLE_PART_COL + "='" + TEMP_TABLE_PART_VAL + "'");
      LOG.info("Created RDD " + rdd.name() + " for table " + tempTableName);
    } catch (IOException e) {
      throw new LensException("Error creating RDD for table " + tempTableName, e);
    }

    return new LensRDDResult(rdd.map(new HCatRecordToObjectListMapper()).rdd(), queryHandle, tempTableName);
  }

  // Create a temp table with schema of the result set and location
  protected String createTempMetastoreTable(String dataLocation,
                                          QueryResultSetMetadata metadata) throws HiveException {
    String tableName = "lens_rdd_" + UUID.randomUUID().toString().replace("-", "_");

    Hive hiveClient = Hive.get(hiveConf);
    Table tbl = hiveClient.newTable("default." + tableName);
    tbl.setTableType(TableType.MANAGED_TABLE);
    tbl.setInputFormatClass(INPUT_FORMAT);
    //String outputFormat = null;
    //tbl.setOutputFormatClass(outputFormat);

    // Add columns
    for (ResultColumn rc : metadata.getColumns()) {
      tbl.getCols().add(new FieldSchema(rc.getName(), toHiveType(rc.getType()), "default"));
      System.out.println("@@@@ COL " + rc.getName() + " TYPE " + toHiveType(rc.getType()));
    }

    tbl.getPartCols().add(new FieldSchema(TEMP_TABLE_PART_COL, "string", "default"));
    hiveClient.createTable(tbl);

    LOG.info("Table " + tableName + " created");

    // Add partition to the table
    AddPartitionDesc partitionDesc = new AddPartitionDesc("default", tableName, false);
    Map<String, String> partSpec = new HashMap<String, String>();
    partSpec.put(TEMP_TABLE_PART_COL, TEMP_TABLE_PART_VAL);
    partitionDesc.addPartition(partSpec, dataLocation);
    hiveClient.createPartitions(partitionDesc);
    LOG.info("Created partition in " + tableName + " for data in " + dataLocation);

    return tableName;
  }

  // Convert lens data type to Hive data type.
  private String toHiveType(ResultColumnType type) {
    return type.name().toLowerCase();
  }

  /**
   * Blocking call to create an RDD from a Lens query. Return only when the query is complete.
   */
  public LensRDDResult createLensRDD(String query) throws LensException {
    QueryHandle queryHandle = createLensRDDAsync(query);
    while (!isReadyForRDD(queryHandle)) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for query", e);
        break;
      }
    }
    return getRDD(queryHandle);
  }

  /**
   * Container object to store the RDD and corresponding Lens query handle.
   */
  public static class LensRDDResult implements Serializable {
    private transient RDD<List<Object>> resultRDD;
    private QueryHandle lensQuery;
    private String tempTableName;

    public LensRDDResult(RDD<List<Object>> rdd, QueryHandle lensQuery, String tempTableName) {
      this.resultRDD = rdd;
      this.lensQuery = lensQuery;
      this.tempTableName = tempTableName;
    }

    public LensRDDResult() {

    }

    public QueryHandle getLensQuery() {
      return lensQuery;
    }

    public RDD<List<Object>> getRDD() {
      return resultRDD;
    }

    /**
     * Recreate RDD. This will work if the result object was saved. As long as the metastore and corresponding
     * HDFS directory is available result object should be able to recreate an RDD.
     * @param sparkContext
     * @return
     * @throws LensException
     */
    public RDD<List<Object>> recreateRDD(JavaSparkContext sparkContext) throws LensException {
      if (resultRDD == null) {
        try {
          JavaPairRDD<WritableComparable, HCatRecord> javaPairRDD = HiveTableRDD.createHiveTableRDD(sparkContext, hiveConf, "default", tempTableName,
            TEMP_TABLE_PART_COL + "='" + TEMP_TABLE_PART_VAL + "'");
          LOG.info("Created RDD " + resultRDD.name() + " for table " + tempTableName);
          resultRDD = javaPairRDD.map(new HCatRecordToObjectListMapper()).rdd();
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
     */
    public void deleteTempTable() throws LensException {
      Hive hiveClient = null;
      try {
        hiveClient = Hive.get(hiveConf);
        hiveClient.dropTable("default." + tempTableName);
        LOG.info("Dropped temp table " + tempTableName);
      } catch (HiveException e) {
        throw new LensException(e);
      }
    }
  }

}