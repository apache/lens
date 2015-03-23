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
import org.apache.lens.api.metastore.ObjectFactory;
import org.apache.lens.client.LensClientSingletonWrapper;
import org.apache.lens.client.LensMetadataClient;

public class SampleMetastore {
  private LensMetadataClient metaClient;
  public static final Unmarshaller JAXB_UNMARSHALLER;
  private APIResult result;
  private int retCode = 0;

  static {
    try {
      JAXBContext jaxbContext;
      jaxbContext = JAXBContext.newInstance(ObjectFactory.class);
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

  public void createCube() throws JAXBException, IOException {
    result = metaClient.createCube("sample-cube.xml");
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.out.println("Creating cube from:sample-cube.xml failed, reason:" + result.getMessage());
      retCode = 1;
    }
  }

  public void createDimensions() throws JAXBException, IOException {
    result = metaClient.createDimension("sample-dimension.xml");
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.out.println("Creating dimension from:sample-dimension.xml failed, reason:" + result.getMessage());
      retCode = 1;
    }

    result = metaClient.createDimension("sample-dimension2.xml");
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.out.println("Creating dimension from:sample-dimension2.xml failed, reason:" + result.getMessage());
      retCode = 1;
    }

    result = metaClient.createDimension("sample-db-only-dimension.xml");
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.out.println("Creating dimension from:sample-db-only-dimension.xml failed, reason:" + result.getMessage());
      retCode = 1;
    }
  }

  private void createStorage(String fileName) throws JAXBException, IOException {
    result = metaClient.createNewStorage(fileName);
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.out.println("Creating storage from:" + fileName + " failed, reason:" + result.getMessage());
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
    createCube();
    createDimensions();
    createFacts();
    createDimensionTables();
  }

  private void createDimensionTables() throws JAXBException, IOException {
    result = metaClient.createDimensionTable("dim_table.xml");
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.out.println("Creating dim table from: dim_table.xml failed, reason:" + result.getMessage());
      retCode = 1;
    }
    result = metaClient.createDimensionTable("dim_table2.xml");
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.out.println("Creating dim table from: dim_table2.xml failed, reason:" + result.getMessage());
      retCode = 1;
    }

    result = metaClient.createDimensionTable("dim_table3.xml");
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.out.println("Creating dim table from: dim_table3.xmlfailed, reason:" + result.getMessage());
      retCode = 1;
    }

    result = metaClient.createDimensionTable("dim_table4.xml");
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.out.println("Creating dim table from: dim_table4.xml failed, reason:" + result.getMessage());
      retCode = 1;
    }
  }

  private void createFacts() throws JAXBException, IOException {
    result = metaClient.createFactTable("fact1.xml");
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.out.println("Creating fact table from: fact1.xml failed, reason:" + result.getMessage());
      retCode = 1;
    }
    result = metaClient.createFactTable("fact2.xml");
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.out.println("Creating fact table from: fact2.xml failed, reason:" + result.getMessage());
      retCode = 1;
    }
    result = metaClient.createFactTable("rawfact.xml");
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.out.println("Creating fact table from: rawfact.xml failed, reason:" + result.getMessage());
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
