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
import org.apache.lens.api.metastore.*;
import org.apache.lens.client.LensClientSingletonWrapper;
import org.apache.lens.client.LensConnection;
import org.apache.lens.client.LensMetadataClient;

public class SampleMetastore {
  private LensConnection connection;
  private LensMetadataClient metaClient;
  public static Unmarshaller jaxbUnmarshaller;
  private APIResult result;
  private int retCode = 0;

  static {
    try {
      JAXBContext jaxbContext;
      jaxbContext = JAXBContext.newInstance(ObjectFactory.class);
      jaxbUnmarshaller = jaxbContext.createUnmarshaller();
    } catch (JAXBException e) {
      throw new RuntimeException("Could not initialize JAXBCOntext");
    }
  }

  public static Object readFromXML(String filename) throws JAXBException, IOException {
    InputStream file = SampleMetastore.class.getClassLoader().getResourceAsStream(filename);
    if (file == null) {
      throw new IOException("File not found:" + filename);
    }
    return ((JAXBElement)jaxbUnmarshaller.unmarshal(file)).getValue();
  }

  public SampleMetastore() throws JAXBException {
    metaClient = new LensMetadataClient(LensClientSingletonWrapper.INSTANCE.getClient().getConnection());
  }

  public void close() {
    LensClientSingletonWrapper.INSTANCE.getClient().closeConnection();
  }

  public void createCube() throws JAXBException, IOException {
    XCube cube = (XCube)readFromXML("sample-cube.xml");
    if (cube != null) {
      result = metaClient.createCube(cube);
      if (result.getStatus().equals(APIResult.Status.FAILED)) {
        System.out.println("Creating cube from:sample-cube.xml failed");
        retCode = 1;
      }
    }
  }

  public void createDimensions() throws JAXBException, IOException {
    XDimension dim1 = (XDimension)readFromXML("sample-dimension.xml");
    if (dim1 != null) {
      result = metaClient.createDimension(dim1);
      if (result.getStatus().equals(APIResult.Status.FAILED)) {
        System.out.println("Creating dimension from:sample-dimension.xml failed");
        retCode = 1;
      }
    }

    XDimension dim2 = (XDimension)readFromXML("sample-dimension2.xml");
    if (dim2 != null) {
      result = metaClient.createDimension(dim2);
      if (result.getStatus().equals(APIResult.Status.FAILED)) {
        System.out.println("Creating dimension from:sample-dimension2.xml failed");
        retCode = 1;
      }
    }

    XDimension dbDim = (XDimension)readFromXML("sample-db-only-dimension.xml");
    if (dbDim != null) {
      result = metaClient.createDimension(dbDim);
      if (result.getStatus().equals(APIResult.Status.FAILED)) {
        System.out.println("Creating dimension from:sample-db-only-dimension.xml failed");
        retCode = 1;
      }
    }

  }

  private void createStorage(String fileName)
      throws JAXBException, IOException {
    XStorage local = (XStorage)readFromXML(fileName);
    if (local != null) {
      result = metaClient.createNewStorage(local);
      if (result.getStatus().equals(APIResult.Status.FAILED)) {
        System.out.println("Creating storage from:" + fileName + " failed");
        retCode = 1;
      }
    }
  }
  public void createStorages() throws JAXBException, IOException {
    createStorage("local-storage.xml");
    createStorage("local-cluster-storage.xml");
    createStorage("db-storage.xml");
  }

  public void createAll() throws JAXBException, IOException {
    createStorages();
    createCube();
    createDimensions();
    createFacts();
    createDimensionTables();
  }

  private void createDimensionTables() throws JAXBException, IOException {
    DimensionTable dim = (DimensionTable)readFromXML("dim_table.xml");
    XStorageTables storageTables = (XStorageTables)readFromXML("dim1-storage-tables.xml");
    if (dim != null && storageTables != null) {
      result = metaClient.createDimensionTable(dim, storageTables);
      if (result.getStatus().equals(APIResult.Status.FAILED)) {
        System.out.println("Creating dim table from: dim_table.xml and dim1-storage-tables.xml failed");
        retCode = 1;
      }
    }
    dim = (DimensionTable)readFromXML("dim_table2.xml");
    storageTables = (XStorageTables)readFromXML("dim2-storage-tables.xml");
    if (dim != null && storageTables != null) {
      result = metaClient.createDimensionTable(dim, storageTables);
      if (result.getStatus().equals(APIResult.Status.FAILED)) {
        System.out.println("Creating dim table from: dim_table2.xml and dim2-storage-tables.xml failed");
        retCode = 1;
      }
    }

    dim = (DimensionTable)readFromXML("dim_table3.xml");
    storageTables = (XStorageTables)readFromXML("dim3-storage-tables.xml");
    if (dim != null && storageTables != null) {
      result = metaClient.createDimensionTable(dim, storageTables);
      if (result.getStatus().equals(APIResult.Status.FAILED)) {
        System.out.println("Creating dim table from: dim_table3.xml and dim3-storage-tables.xml failed");
        retCode = 1;
      }
    }

    dim = (DimensionTable)readFromXML("dim_table4.xml");
    storageTables = (XStorageTables)readFromXML("dim4-storage-tables.xml");
    if (dim != null && storageTables != null) {
      result = metaClient.createDimensionTable(dim, storageTables);
      if (result.getStatus().equals(APIResult.Status.FAILED)) {
        System.out.println("Creating dim table from: dim_table4.xml and dim4-storage-tables.xml failed");
        retCode = 1;
      }
    }
  }

  private void createFacts() throws JAXBException, IOException {
    FactTable fact = (FactTable)readFromXML("fact1.xml");
    XStorageTables storageTables = (XStorageTables)readFromXML("fact1-storage-tables.xml");
    if (fact != null && storageTables != null) {
      result = metaClient.createFactTable(fact, storageTables);
      if (result.getStatus().equals(APIResult.Status.FAILED)) {
        System.out.println("Creating fact table from: fact1.xml and fact1-storage-tables.xml failed");
        retCode = 1;
      }
    }
    fact = (FactTable)readFromXML("fact2.xml");
    storageTables = (XStorageTables)readFromXML("fact2-storage-tables.xml");
    if (fact != null && storageTables != null) {
      result = metaClient.createFactTable(fact, storageTables);
      if (result.getStatus().equals(APIResult.Status.FAILED)) {
        System.out.println("Creating fact table from: fact2.xml and fact2-storage-tables.xml failed");
        retCode = 1;
      }
    }
    fact = (FactTable)readFromXML("rawfact.xml");
    storageTables = (XStorageTables)readFromXML("rawfact-storage-tables.xml");
    if (fact != null && storageTables != null) {
      result = metaClient.createFactTable(fact, storageTables);
      if (result.getStatus().equals(APIResult.Status.FAILED)) {
        System.out.println("Creating fact table from: rawfact.xml and rawfact-storage-tables.xml failed");
        retCode = 1;
      }
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
      System.out.println("Database:" + metastore.metaClient.getCurrentDatabase());;
      System.out.println("Storages:" + metastore.metaClient.getAllStorages());;
      System.out.println("Cubes:" + metastore.metaClient.getAllCubes());;
      System.out.println("Dimensions:" + metastore.metaClient.getAllDimensions());;
      System.out.println("Fact tables:" + metastore.metaClient.getAllFactTables());;
      System.out.println("Dimension tables:" + metastore.metaClient.getAllDimensionTables());
      if (metastore.retCode != 0) {
        System.exit(metastore.retCode);
      }
    } finally {
      if (metastore != null) {
        metastore.close();
      }
    }
  }
}
