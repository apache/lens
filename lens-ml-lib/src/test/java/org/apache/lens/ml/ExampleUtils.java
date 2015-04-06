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
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * The Class ExampleUtils.
 */
public final class ExampleUtils {
  private ExampleUtils() {
  }

  private static final Log LOG = LogFactory.getLog(ExampleUtils.class);

  /**
   * Creates the example table.
   *
   * @param conf           the conf
   * @param database       the database
   * @param tableName      the table name
   * @param sampleDataFile the sample data file
   * @param labelColumn    the label column
   * @param features       the features
   * @throws HiveException the hive exception
   */
  public static void createTable(HiveConf conf, String database, String tableName, String sampleDataFile,
    String labelColumn, Map<String, String> tableParams, String... features) throws HiveException {

    Path dataFilePath = new Path(sampleDataFile);
    Path partDir = dataFilePath.getParent();

    // Create table
    List<FieldSchema> columns = new ArrayList<FieldSchema>();

    // Label is optional. Not used for unsupervised models.
    // If present, label will be the first column, followed by features
    if (labelColumn != null) {
      columns.add(new FieldSchema(labelColumn, "double", "Labelled Column"));
    }

    for (String feature : features) {
      columns.add(new FieldSchema(feature, "double", "Feature " + feature));
    }

    Table tbl = Hive.get(conf).newTable(database + "." + tableName);
    tbl.setTableType(TableType.MANAGED_TABLE);
    tbl.getTTable().getSd().setCols(columns);
    tbl.getTTable().getParameters().putAll(tableParams);
    tbl.setInputFormatClass(TextInputFormat.class);
    tbl.setSerdeParam(serdeConstants.LINE_DELIM, "\n");
    tbl.setSerdeParam(serdeConstants.FIELD_DELIM, " ");

    List<FieldSchema> partCols = new ArrayList<FieldSchema>(1);
    partCols.add(new FieldSchema("dummy_partition_col", "string", ""));
    tbl.setPartCols(partCols);

    Hive.get(conf).createTable(tbl, false);
    LOG.info("Created table " + tableName);

    // Add partition for the data file
    AddPartitionDesc partitionDesc = new AddPartitionDesc(database, tableName, false);
    Map<String, String> partSpec = new HashMap<String, String>();
    partSpec.put("dummy_partition_col", "dummy_val");
    partitionDesc.addPartition(partSpec, partDir.toUri().toString());
    Hive.get(conf).createPartitions(partitionDesc);
    LOG.info(tableName + ": Added partition " + partDir.toUri().toString());
  }
}
