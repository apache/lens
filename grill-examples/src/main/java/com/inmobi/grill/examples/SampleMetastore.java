package com.inmobi.grill.examples;

import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import com.inmobi.grill.api.metastore.DimensionTable;
import com.inmobi.grill.api.metastore.FactTable;
import com.inmobi.grill.api.metastore.ObjectFactory;
import com.inmobi.grill.api.metastore.XCube;
import com.inmobi.grill.api.metastore.XStorage;
import com.inmobi.grill.api.metastore.XStorageTables;
import com.inmobi.grill.client.GrillConnection;
import com.inmobi.grill.client.GrillConnectionParams;
import com.inmobi.grill.client.GrillMetadataClient;

public class SampleMetastore {
  private GrillConnection connection;
  private GrillMetadataClient metaClient;
  private JAXBContext jaxbContext;
  private Unmarshaller jaxbUnmarshaller;

  public SampleMetastore() throws JAXBException {
    GrillConnectionParams params = new GrillConnectionParams();
    connection = new GrillConnection(params);
    connection.open();
    
    metaClient = new GrillMetadataClient(connection);
    jaxbContext = JAXBContext.newInstance(ObjectFactory.class);
    jaxbUnmarshaller = jaxbContext.createUnmarshaller();
  }

  public void close() {
    connection.close();
  }

  public void createCube() throws JAXBException {
    XCube cube = (XCube)readFromXML("sample-cube.xml");
    if (cube != null) {
      metaClient.createCube(cube);
    }
  }

  private Object readFromXML(String filename) throws JAXBException {
    InputStream file = getClass().getClassLoader().getResourceAsStream(filename);
    if (file == null) {
      System.out.println("File not found:" + filename);
      return null;
    }
    return ((JAXBElement)jaxbUnmarshaller.unmarshal(file)).getValue();
  }

  public void createStorages() throws JAXBException {
    XStorage local = (XStorage)readFromXML("local-storage.xml");
    if (local != null) {
      metaClient.createNewStorage(local);
    }

    XStorage cluster = (XStorage)readFromXML("local-cluster-storage.xml");
    if (cluster != null) {
      metaClient.createNewStorage(cluster);
    }

    XStorage db = (XStorage)readFromXML("db-storage.xml");
    if (db != null) {
      metaClient.createNewStorage(db);
    }
  }

  public void createAll() throws JAXBException {
    createStorages();
    createCube();
    //createFacts();
    //createDimensions();
  }

  private void createDimensions() throws JAXBException {
    DimensionTable dim = (DimensionTable)readFromXML("dim_table.xml");
    if (dim != null) {
      metaClient.createDimensionTable(dim, new XStorageTables());
    }
    dim = (DimensionTable)readFromXML("dim_table2.xml");
    if (dim != null) {
      metaClient.createDimensionTable(dim, new XStorageTables());
    }
  }

  private void createFacts() throws JAXBException {
    FactTable fact = (FactTable)readFromXML("fact1.xml");
    if (fact != null) {
      metaClient.createFactTable(fact, new XStorageTables());
    }
    fact = (FactTable)readFromXML("fact2.xml");
    if (fact != null) {
      metaClient.createFactTable(fact, new XStorageTables());
    }
    fact = (FactTable)readFromXML("rawfact.xml");
    if (fact != null) {
      metaClient.createFactTable(fact, new XStorageTables());
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
    System.out.println("Fact tables:" + metastore.metaClient.getAllFactTables());;
    System.out.println("Dimension tables:" + metastore.metaClient.getAllDimensionTables());
    } finally {
      if (metastore != null) {
        metastore.close();
      }
    }
  }
}
