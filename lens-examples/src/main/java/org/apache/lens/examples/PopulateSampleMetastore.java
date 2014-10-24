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

import javax.xml.bind.JAXBException;

import org.apache.lens.api.APIResult;
import org.apache.lens.client.*;

import org.apache.lens.api.metastore.XPartition;

public class PopulateSampleMetastore {
  private LensMetadataClient metaClient;
  private APIResult result;
  private int retCode = 0;

  public PopulateSampleMetastore() throws JAXBException {
    metaClient = new LensMetadataClient(LensClientSingletonWrapper.INSTANCE.getClient().getConnection());
  }

  public void close() {
    LensClientSingletonWrapper.INSTANCE.getClient().closeConnection();
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
  }

  public void populateAll() throws JAXBException, IOException {
    populateDimTables();
    populateFactTables();
  }

  public void populateDimTables() throws JAXBException, IOException {
    XPartition partition = (XPartition)SampleMetastore.readFromXML("dim1-local-part.xml");
    String partLocation = partition.getLocation();
    if (!partLocation.startsWith("/")) {
      partition.setLocation("file://" + System.getProperty("lens.home") + "/" + partLocation);
    }
    result = metaClient.addPartitionToDimensionTable("dim_table", "local", partition);
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.out.println("Adding partition from:dim1-local-part.xml failed");
      retCode = 1;
    } else {
      System.out.println("Added partition from:dim1-local-part.xml");
    }
    partition = (XPartition)SampleMetastore.readFromXML("dim2-local-part.xml");
    partLocation = partition.getLocation();
    if (!partLocation.startsWith("/")) {
      partition.setLocation("file://" + System.getProperty("lens.home") + "/" + partLocation);
    }
    result = metaClient.addPartitionToDimensionTable("dim_table2", "local", partition);
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.out.println("Adding partition from:dim2-local-part.xml failed");
      retCode = 1;
    } else {
      System.out.println("Added partition from:dim2-local-part.xml");
    }

    partition = (XPartition)SampleMetastore.readFromXML("dim4-local-part.xml");
    partLocation = partition.getLocation();
    if (!partLocation.startsWith("/")) {
      partition.setLocation("file://" + System.getProperty("lens.home") + "/" + partLocation);
    }
    result = metaClient.addPartitionToDimensionTable("dim_table4", "local", partition);
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.out.println("Adding partition from:dim4-local-part.xml failed");
      retCode = 1;
    } else {
      System.out.println("Added partition from:dim4-local-part.xml");
    }

    try {
      DatabaseUtil.initalizeDatabaseStorage();
      System.out.println("Created DB storages for dim_table3 and dim_table4");
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Creating DB storage failed for dim_table3 and dim_table4");
    }
  }

  private void createFactPartition(String fileName, String fact, String storage) throws JAXBException, IOException {
    XPartition partition = (XPartition)SampleMetastore.readFromXML(fileName);
    String partLocation = partition.getLocation();
    if (!partLocation.startsWith("/")) {
      partition.setLocation("file://" + System.getProperty("lens.home") + "/" + partLocation);
    }
    result = metaClient.addPartitionToFactTable(fact, storage, partition);
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.out.println("Adding partition from:" + fileName + " failed");
      retCode = 1;
    } else {
      System.out.println("Added partition from:" + fileName);
    }
  }

  public void populateFactTables() throws JAXBException, IOException {
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
  }

}
