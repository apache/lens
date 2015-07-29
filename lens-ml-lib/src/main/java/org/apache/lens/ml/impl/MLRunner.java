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
package org.apache.lens.ml.impl;

import java.io.File;
import java.io.FileInputStream;
import java.util.*;

import org.apache.lens.client.LensClient;
import org.apache.lens.client.LensClientConfig;
import org.apache.lens.client.LensMLClient;

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

public class MLRunner {

  private static final Log LOG = LogFactory.getLog(MLRunner.class);

  private LensMLClient mlClient;
  private String algoName;
  private String database;
  private String trainTable;
  private String trainFile;
  private String testTable;
  private String testFile;
  private String outputTable;
  private String[] features;
  private String labelColumn;
  private HiveConf conf;

  public void init(LensMLClient mlClient, String confDir) throws Exception {
    File dir = new File(confDir);
    File propFile = new File(dir, "ml.properties");
    Properties props = new Properties();
    props.load(new FileInputStream(propFile));
    String feat = props.getProperty("features");
    String trainFile = confDir + File.separator + "train.data";
    String testFile = confDir + File.separator + "test.data";
    init(mlClient, props.getProperty("algo"), props.getProperty("database"),
        props.getProperty("traintable"), trainFile,
        props.getProperty("testtable"), testFile,
        props.getProperty("outputtable"), feat.split(","),
        props.getProperty("labelcolumn"));
  }

  public void init(LensMLClient mlClient, String algoName,
      String database, String trainTable, String trainFile,
      String testTable, String testFile, String outputTable, String[] features,
      String labelColumn) {
    this.mlClient = mlClient;
    this.algoName = algoName;
    this.database = database;
    this.trainTable = trainTable;
    this.trainFile = trainFile;
    this.testTable = testTable;
    this.testFile = testFile;
    this.outputTable = outputTable;
    this.features = features;
    this.labelColumn = labelColumn;
    //hive metastore settings are loaded via lens-site.xml, so loading LensClientConfig
    //is required
    this.conf = new HiveConf(new LensClientConfig(), MLRunner.class);
  }

  public MLTask train() throws Exception {
    LOG.info("Starting train & eval");

    createTable(trainTable, trainFile);
    createTable(testTable, testFile);
    MLTask.Builder taskBuilder = new MLTask.Builder();
    taskBuilder.algorithm(algoName).hiveConf(conf).labelColumn(labelColumn).outputTable(outputTable)
        .client(mlClient).trainingTable(trainTable).testTable(testTable);

    // Add features
    for (String feature : features) {
      taskBuilder.addFeatureColumn(feature);
    }
    MLTask task = taskBuilder.build();
    LOG.info("Created task " + task.toString());
    task.run();
    return task;
  }

  public void createTable(String tableName, String dataFile) throws HiveException {

    File filedataFile = new File(dataFile);
    Path dataFilePath = new Path(filedataFile.toURI());
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
    // tbl.getTTable().getParameters().putAll(new HashMap<String, String>());
    tbl.setInputFormatClass(TextInputFormat.class);
    tbl.setSerdeParam(serdeConstants.LINE_DELIM, "\n");
    tbl.setSerdeParam(serdeConstants.FIELD_DELIM, " ");

    List<FieldSchema> partCols = new ArrayList<FieldSchema>(1);
    partCols.add(new FieldSchema("dummy_partition_col", "string", ""));
    tbl.setPartCols(partCols);

    Hive.get(conf).dropTable(database, tableName, false, true);
    Hive.get(conf).createTable(tbl, true);
    LOG.info("Created table " + tableName);

    // Add partition for the data file
    AddPartitionDesc partitionDesc = new AddPartitionDesc(database, tableName,
        false);
    Map<String, String> partSpec = new HashMap<String, String>();
    partSpec.put("dummy_partition_col", "dummy_val");
    partitionDesc.addPartition(partSpec, partDir.toUri().toString());
    Hive.get(conf).createPartitions(partitionDesc);
    LOG.info(tableName + ": Added partition " + partDir.toUri().toString());
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.out.println("Usage: " + MLRunner.class.getName() + " <ml-conf-dir>");
      System.exit(-1);
    }
    String confDir = args[0];
    LensMLClient client = new LensMLClient(new LensClient());
    MLRunner runner = new MLRunner();
    runner.init(client, confDir);
    runner.train();
    System.out.println("Created the Model successfully. Output Table: " + runner.outputTable);
  }
}
