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
package org.apache.lens.examples;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.bind.JAXBException;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.metastore.XPartition;
import org.apache.lens.api.metastore.XPartitionList;
import org.apache.lens.client.LensClientSingletonWrapper;
import org.apache.lens.client.LensMetadataClient;


public class PopulateSampleMetastore {
  private LensMetadataClient metaClient;
  private APIResult result;
  private int retCode = 0;

  private static final Date DATE = new Date(System.currentTimeMillis());
  private static final SimpleDateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private static final String NOW_TIME = FORMAT.format(DATE);

  private static final String INSERT_QUERY = "INSERT INTO "
      + "mydb_sales_aggr_continuous_fact (order_time, delivery_time, customer_id, "
      + "product_id, promotion_id, customer_city_id, production_city_id, delivery_city_id, unit_sales, "
      + "store_sales, store_cost, max_line_item_price, max_line_item_discount) values "
      + "('" + NOW_TIME + "','" + NOW_TIME + "',2,2,1,2,2,2,1,8,2,10,2)";

  public PopulateSampleMetastore() throws JAXBException {
    metaClient = new LensMetadataClient(LensClientSingletonWrapper.instance().getClient().getConnection());
  }

  public void close() {
    LensClientSingletonWrapper.instance().getClient().closeConnection();
  }

  public static void main(String[] args) throws Exception {
    PopulateSampleMetastore populate = null;
    try {
      populate = new PopulateSampleMetastore();
      if (args.length > 0) {
        if (args[0].equals("-db")) {
          String dbName = args[1];
          populate.metaClient.createDatabase(dbName, true);
          populate.metaClient.setDatabase(dbName);
        }
      }
      populate.populateAll();
    } finally {
      if (populate != null) {
        populate.close();
      }
    }
    if (populate.retCode != 0) {
      System.exit(populate.retCode);
    }
  }

  public void populateAll() throws Exception {
    populateDimTables();
    populateFactTables();
  }

  public void populateDimTables() throws JAXBException, IOException {
    createDimTablePartition("dim1-local-part.xml", "dim_table", "local");
    createDimTablePartition("dim2-local-part.xml", "dim_table2", "local");
    createDimTablePartition("dim4-local-part.xml", "dim_table4", "local");
    createDimTablePartitions("product-local-parts.xml", "product_table", "local");
    createDimTablePartition("city-local-part.xml", "city_table", "local");
    createDimTablePartition("customer-local-part.xml", "customer_table", "local");
    createDimTablePartition("customer-interests-local-part.xml", "customer_interests_table", "local");
    createDimTablePartition("interests-local-part.xml", "interests_table", "local");
  }

  private void createDimTablePartition(String fileName, String dimTable, String storage)
    throws JAXBException, IOException {
    XPartition partition = (XPartition) SampleMetastore.readFromXML(fileName);
    String partLocation = partition.getLocation();
    if (!partLocation.startsWith("/")) {
      partition.setLocation("file://" + System.getProperty("lens.home") + "/" + partLocation);
    }
    result = metaClient.addPartitionToDimensionTable(dimTable, storage, partition);
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.err.println("Adding partition from:"+ fileName + " failed");
      retCode = 1;
    } else {
      System.out.println("Added partition from:" + fileName);
    }
  }

  private void createContinuousFactData() throws Exception {
    Class.forName("org.hsqldb.jdbcDriver");
    Connection con = DriverManager.getConnection(
        "jdbc:hsqldb:/tmp/db-storage.db", "SA", "");

    con.setAutoCommit(true);
    Statement statement = con.createStatement();
    try {
      statement.execute(INSERT_QUERY);

    } finally {
      statement.close();
      con.close();
    }

  }

  private void createDimTablePartitions(String fileName, String dimTable, String storage)
    throws JAXBException, IOException {
    XPartitionList partitionList = (XPartitionList) SampleMetastore.readFromXML(fileName);
    for (XPartition partition : partitionList.getPartition()) {
      String partLocation = partition.getLocation();
      if (!partLocation.startsWith("/")) {
        partition.setLocation("file://" + System.getProperty("lens.home") + "/" + partLocation);
      }
    }
    result = metaClient.addPartitionsToDimensionTable(dimTable, storage, partitionList);
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.err.println("Adding partitions from:" + fileName + " failed");
      retCode = 1;
    } else {
      System.out.println("Added partitions from:" + fileName);
    }
  }

  private void createFactPartition(String fileName, String fact, String storage) throws JAXBException, IOException {
    XPartition partition = (XPartition) SampleMetastore.readFromXML(fileName);
    String partLocation = partition.getLocation();
    if (!partLocation.startsWith("/")) {
      partition.setLocation("file://" + System.getProperty("lens.home") + "/" + partLocation);
    }
    result = metaClient.addPartitionToFactTable(fact, storage, partition);
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.err.println("Adding partition from:" + fileName + " failed");
      retCode = 1;
    } else {
      System.out.println("Added partition from:" + fileName);
    }
  }

  private void createFactPartitions(String fileName, String fact, String storage) throws JAXBException, IOException {
    XPartitionList partitionList = (XPartitionList) SampleMetastore.readFromXML(fileName);
    for (XPartition partition : partitionList.getPartition()) {
      String partLocation = partition.getLocation();
      if (!partLocation.startsWith("/")) {
        partition.setLocation("file://" + System.getProperty("lens.home") + "/" + partLocation);
      }
    }
    result = metaClient.addPartitionsToFactTable(fact, storage, partitionList);
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.err.println("Adding partitions from:" + fileName + " failed");
      retCode = 1;
    } else {
      System.out.println("Added partitions from:" + fileName);
    }
  }

  public void populateFactTables() throws Exception {
    createFactPartition("fact1-local-part1.xml", "fact1", "local");
    createFactPartition("fact1-local-part2.xml", "fact1", "local");
    createFactPartition("fact1-local-part3.xml", "fact1", "local");
    createFactPartition("fact2-local-part1.xml", "fact2", "local");
    createFactPartition("fact2-local-part2.xml", "fact2", "local");
    createFactPartition("fact2-local-part3.xml", "fact2", "local");
    createFactPartition("raw-local-part1.xml", "rawfact", "local");
    createFactPartition("raw-local-part2.xml", "rawfact", "local");
    createFactPartition("raw-local-part3.xml", "rawfact", "local");
    createFactPartition("raw-local-part4.xml", "rawfact", "local");
    createFactPartition("raw-local-part5.xml", "rawfact", "local");
    createFactPartition("raw-local-part6.xml", "rawfact", "local");
    createFactPartitions("sales-raw-local-parts.xml", "sales_raw_fact", "local");
    createFactPartitions("sales-aggr-fact1-local-parts.xml", "sales_aggr_fact1", "local");
    createFactPartitions("sales-aggr-fact2-local-parts.xml", "sales_aggr_fact2", "local");
    createFactPartitions("sales-aggr-fact1-mydb-parts.xml", "sales_aggr_fact1", "mydb");
    createFactPartitions("sales-aggr-fact2-mydb-parts.xml", "sales_aggr_fact2", "mydb");
    createContinuousFactData();
  }
}
