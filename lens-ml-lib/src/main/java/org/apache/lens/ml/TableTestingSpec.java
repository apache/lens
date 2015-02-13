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
package org.apache.lens.ml;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

import lombok.Getter;

/**
 * Table specification for running test on a table.
 */
public class TableTestingSpec {

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(TableTestingSpec.class);

  /** The db. */
  private String db;

  /** The table containing input data. */
  private String inputTable;

  // TODO use partition condition
  /** The partition filter. */
  private String partitionFilter;

  /** The feature columns. */
  private List<String> featureColumns;

  /** The label column. */
  private String labelColumn;

  /** The output column. */
  private String outputColumn;

  /** The output table. */
  private String outputTable;

  /** The conf. */
  private transient HiveConf conf;

  /** The algorithm. */
  private String algorithm;

  /** The model id. */
  private String modelID;

  @Getter
  private boolean outputTableExists;

  @Getter
  private String testID;

  private HashMap<String, FieldSchema> columnNameToFieldSchema;

  /**
   * The Class TableTestingSpecBuilder.
   */
  public static class TableTestingSpecBuilder {

    /** The spec. */
    private final TableTestingSpec spec;

    /**
     * Instantiates a new table testing spec builder.
     */
    public TableTestingSpecBuilder() {
      spec = new TableTestingSpec();
    }

    /**
     * Database.
     *
     * @param database the database
     * @return the table testing spec builder
     */
    public TableTestingSpecBuilder database(String database) {
      spec.db = database;
      return this;
    }

    /**
     * Set the input table
     *
     * @param table the table
     * @return the table testing spec builder
     */
    public TableTestingSpecBuilder inputTable(String table) {
      spec.inputTable = table;
      return this;
    }

    /**
     * Partition filter for input table
     *
     * @param partFilter the part filter
     * @return the table testing spec builder
     */
    public TableTestingSpecBuilder partitionFilter(String partFilter) {
      spec.partitionFilter = partFilter;
      return this;
    }

    /**
     * Feature columns.
     *
     * @param featureColumns the feature columns
     * @return the table testing spec builder
     */
    public TableTestingSpecBuilder featureColumns(List<String> featureColumns) {
      spec.featureColumns = featureColumns;
      return this;
    }

    /**
     * Labe column.
     *
     * @param labelColumn the label column
     * @return the table testing spec builder
     */
    public TableTestingSpecBuilder lableColumn(String labelColumn) {
      spec.labelColumn = labelColumn;
      return this;
    }

    /**
     * Output column.
     *
     * @param outputColumn the output column
     * @return the table testing spec builder
     */
    public TableTestingSpecBuilder outputColumn(String outputColumn) {
      spec.outputColumn = outputColumn;
      return this;
    }

    /**
     * Output table.
     *
     * @param table the table
     * @return the table testing spec builder
     */
    public TableTestingSpecBuilder outputTable(String table) {
      spec.outputTable = table;
      return this;
    }

    /**
     * Hive conf.
     *
     * @param conf the conf
     * @return the table testing spec builder
     */
    public TableTestingSpecBuilder hiveConf(HiveConf conf) {
      spec.conf = conf;
      return this;
    }

    /**
     * Algorithm.
     *
     * @param algorithm the algorithm
     * @return the table testing spec builder
     */
    public TableTestingSpecBuilder algorithm(String algorithm) {
      spec.algorithm = algorithm;
      return this;
    }

    /**
     * Model id.
     *
     * @param modelID the model id
     * @return the table testing spec builder
     */
    public TableTestingSpecBuilder modelID(String modelID) {
      spec.modelID = modelID;
      return this;
    }

    /**
     * Builds the.
     *
     * @return the table testing spec
     */
    public TableTestingSpec build() {
      return spec;
    }

    /**
     * Set the unique test id
     *
     * @param testID
     * @return
     */
    public TableTestingSpecBuilder testID(String testID) {
      spec.testID = testID;
      return this;
    }
  }

  /**
   * New builder.
   *
   * @return the table testing spec builder
   */
  public static TableTestingSpecBuilder newBuilder() {
    return new TableTestingSpecBuilder();
  }

  /**
   * Validate.
   *
   * @return true, if successful
   */
  public boolean validate() {
    List<FieldSchema> columns;
    try {
      Hive metastoreClient = Hive.get(conf);
      Table tbl = (db == null) ? metastoreClient.getTable(inputTable) : metastoreClient.getTable(db, inputTable);
      columns = tbl.getAllCols();
      columnNameToFieldSchema = new HashMap<String, FieldSchema>();

      for (FieldSchema fieldSchema : columns) {
        columnNameToFieldSchema.put(fieldSchema.getName(), fieldSchema);
      }

      // Check if output table exists
      Table outTbl = metastoreClient.getTable(db == null ? "default" : db, outputTable, false);
      outputTableExists = (outTbl != null);
    } catch (HiveException exc) {
      LOG.error("Error getting table info " + toString(), exc);
      return false;
    }

    // Check if labeled column and feature columns are contained in the table
    List<String> testTableColumns = new ArrayList<String>(columns.size());
    for (FieldSchema column : columns) {
      testTableColumns.add(column.getName());
    }

    if (!testTableColumns.containsAll(featureColumns)) {
      LOG.info("Invalid feature columns: " + featureColumns + ". Actual columns in table:" + testTableColumns);
      return false;
    }

    if (!testTableColumns.contains(labelColumn)) {
      LOG.info("Invalid label column: " + labelColumn + ". Actual columns in table:" + testTableColumns);
      return false;
    }

    if (StringUtils.isBlank(outputColumn)) {
      LOG.info("Output column is required");
      return false;
    }

    if (StringUtils.isBlank(outputTable)) {
      LOG.info("Output table is required");
      return false;
    }
    return true;
  }

  public String getTestQuery() {
    if (!validate()) {
      return null;
    }

    // We always insert a dynamic partition
    StringBuilder q = new StringBuilder("INSERT OVERWRITE TABLE " + outputTable + " PARTITION (part_testid='" + testID
      + "')  SELECT ");
    String featureCols = StringUtils.join(featureColumns, ",");
    q.append(featureCols).append(",").append(labelColumn).append(", ").append("predict(").append("'").append(algorithm)
      .append("', ").append("'").append(modelID).append("', ").append(featureCols).append(") ").append(outputColumn)
      .append(" FROM ").append(inputTable);

    return q.toString();
  }

  public String getCreateOutputTableQuery() {
    StringBuilder createTableQuery = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(outputTable).append("(");
    // Output table contains feature columns, label column, output column
    List<String> outputTableColumns = new ArrayList<String>();
    for (String featureCol : featureColumns) {
      outputTableColumns.add(featureCol + " " + columnNameToFieldSchema.get(featureCol).getType());
    }

    outputTableColumns.add(labelColumn + " " + columnNameToFieldSchema.get(labelColumn).getType());
    outputTableColumns.add(outputColumn + " string");

    createTableQuery.append(StringUtils.join(outputTableColumns, ", "));

    // Append partition column
    createTableQuery.append(") PARTITIONED BY (part_testid string)");

    return createTableQuery.toString();
  }
}
