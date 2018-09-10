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
import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.jaxb.LensJAXBContext;
import org.apache.lens.api.metastore.ObjectFactory;
import org.apache.lens.client.LensClientSingletonWrapper;
import org.apache.lens.client.LensMetadataClient;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SampleMetastore {

  private LensMetadataClient metaClient;
  public static final Unmarshaller JAXB_UNMARSHALLER;
  private APIResult result;
  private int retCode = 0;

  static {
    try {
      JAXBContext jaxbContext;
      jaxbContext = new LensJAXBContext(ObjectFactory.class);
      JAXB_UNMARSHALLER = jaxbContext.createUnmarshaller();
    } catch (JAXBException e) {
      throw new RuntimeException("Could not initialize JAXBCOntext");
    }
  }

  public static Object readFromXML(String filename) throws JAXBException, IOException {
    InputStream file = SampleMetastore.class.getClassLoader().getResourceAsStream(filename);
    if (file == null) {
      throw new IOException("File not found:" + filename);
    }
    return ((JAXBElement) JAXB_UNMARSHALLER.unmarshal(file)).getValue();
  }

  public SampleMetastore() throws JAXBException {
    metaClient = new LensMetadataClient(LensClientSingletonWrapper.instance().getClient().getConnection());
  }

  public void close() {
    LensClientSingletonWrapper.instance().getClient().closeConnection();
  }

  private void createCube(String cubeSpec) {
    result = metaClient.createCube(cubeSpec);
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.err.println("Creating cube from:" + cubeSpec + " failed, reason:" + result.getMessage());
      retCode = 1;
    }
  }

  public void createCubes() throws JAXBException, IOException {
    createCube("sample-cube.xml");
    createCube("sales-cube.xml");
    createCube("cube11.xml");
    createCube("cube22.xml");
    createCube("cube33.xml");
  }

  private void createDimension(String dimensionSpec) {
    result = metaClient.createDimension(dimensionSpec);
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.err.println("Creating dimension from:" + dimensionSpec + " failed, reason:" + result.getMessage());
      retCode = 1;
    }
  }

  public void createDimensions() throws JAXBException, IOException {
    createDimension("sample-dimension.xml");
    createDimension("sample-dimension2.xml");
    createDimension("sample-db-only-dimension.xml");
    createDimension("city.xml");
    createDimension("customer.xml");
    createDimension("product.xml");
    createDimension("customer-interests.xml");
    createDimension("interests.xml");
  }

  private void createStorage(String fileName) throws JAXBException, IOException {
    result = metaClient.createNewStorage(fileName);
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.err.println("Creating storage from:" + fileName + " failed, reason:" + result.getMessage());
      retCode = 1;
    }
  }

  public void createStorages() throws JAXBException, IOException {
    createStorage("local-storage.xml");
    createStorage("local-cluster-storage.xml");
    createStorage("db-storage.xml");
  }

  public void createAll() throws JAXBException, IOException {
    createStorages();
    createCubes();
    createDimensions();
    createFacts();
    createSegmentations();
    createDimensionTables();
    try {
      DatabaseUtil.initializeDatabaseStorage();
      System.out.println("Created DB storages");
    } catch (Exception e) {
      retCode = 1;
      log.error("Creating DB storage failed", e);
      System.err.println("Creating DB storage failed");
    }
  }

  private void createDimTable(String dimTableSpec) {
    result = metaClient.createDimensionTable(dimTableSpec);
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.err.println("Creating dim table from: " + dimTableSpec + " failed, reason:" + result.getMessage());
      retCode = 1;
    }
  }

  private void createDimensionTables() throws JAXBException, IOException {
    createDimTable("dim_table.xml");
    createDimTable("dim_table2.xml");
    createDimTable("dim_table3.xml");
    createDimTable("dim_table4.xml");
    createDimTable("city_table.xml");
    createDimTable("city_subset.xml");
    createDimTable("product_table.xml");
    createDimTable("product_db_table.xml");
    createDimTable("customer_table.xml");
    createDimTable("customer_interests_table.xml");
    createDimTable("interests_table.xml");
  }

  private void createFact(String factSpec) {
    result = metaClient.createFactTable(factSpec);
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.err.println("Creating fact table from: " + factSpec + " failed, reason:" + result.getMessage());
      retCode = 1;
    }
  }

  private void createFacts() throws JAXBException, IOException {
    createFact("fact1.xml");
    createFact("fact2.xml");
    createFact("fact3.xml");
    createFact("rawfact.xml");
    createFact("sales-raw-fact.xml");
    createFact("sales-aggr-fact1.xml");
    createFact("sales-aggr-fact2.xml");
    createFact("sales-aggr-continuous-fact.xml");
  }

  private void createSegmentations() throws JAXBException, IOException {
    result = metaClient.createSegmentation("seg1.xml");
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.err.println("Creating segmentation from : " + "seg1.xml"
          + " failed, reason:" + result.getMessage());
      retCode = 1;
    }
  }

  public static void main(String[] args) throws Exception {
    SampleMetastore metastore = null;
    try {
      metastore = new SampleMetastore();
      if (args.length > 0) {
        if (args[0].equals("-db")) {
          String dbName = args[1];
          metastore.metaClient.createDatabase(dbName, true);
          metastore.metaClient.setDatabase(dbName);
        }
      }
      metastore.createAll();
      System.out.println("Created sample metastore!");
      System.out.println("Database:" + metastore.metaClient.getCurrentDatabase());
      System.out.println("Storages:" + metastore.metaClient.getAllStorages());
      System.out.println("Cubes:" + metastore.metaClient.getAllCubes());
      System.out.println("Dimensions:" + metastore.metaClient.getAllDimensions());
      System.out.println("Fact tables:" + metastore.metaClient.getAllFactTables());
      System.out.println("Dimension tables:" + metastore.metaClient.getAllDimensionTables());
      System.out.println("Segmentations:" + metastore.metaClient.getAllSegmentations());
      if (metastore.retCode != 0) {
        System.exit(metastore.retCode);
      }
    } catch (Throwable th) {
      log.error("Error during creating sample metastore", th);
      throw th;
    } finally {
      if (metastore != null) {
        metastore.close();
      }
    }
  }
}
