package com.inmobi.grill.examples;

import java.io.IOException;

import javax.xml.bind.JAXBException;

import com.inmobi.grill.api.APIResult;
import com.inmobi.grill.api.metastore.XPartition;
import com.inmobi.grill.client.GrillConnection;
import com.inmobi.grill.client.GrillConnectionParams;
import com.inmobi.grill.client.GrillMetadataClient;

public class PopulateSampleMetastore {

  private GrillConnection connection;
  private GrillMetadataClient metaClient;
  private APIResult result;
  private int retCode = 0;

  public PopulateSampleMetastore() throws JAXBException {
    connection = new GrillConnection(new GrillConnectionParams());
    connection.open();
    metaClient = new GrillMetadataClient(connection);
  }

  public void close() {
    connection.close();
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
  }

  public void populateDimTables() throws JAXBException, IOException {
    XPartition partition = (XPartition)SampleMetastore.readFromXML("dim1-local-part.xml");
    String partLocation = partition.getLocation();
    if (!partLocation.startsWith("/")) {
      partition.setLocation(System.getProperty("grill.home") + "/" + partLocation);
    }
    result = metaClient.addPartitionToDimension("dim_table", "local", partition);
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.out.println("Adding partition from:dim1-local-part.xml failed");
      retCode = 1;
    } else {
      System.out.println("Added partition from:dim1-local-part.xml");
    }
    partition = (XPartition)SampleMetastore.readFromXML("dim2-local-part.xml");
    partLocation = partition.getLocation();
    if (!partLocation.startsWith("/")) {
      partition.setLocation(System.getProperty("grill.home") + "/" + partLocation);
    }
    result = metaClient.addPartitionToDimension("dim_table2", "local", partition);
    if (result.getStatus().equals(APIResult.Status.FAILED)) {
      System.out.println("Adding partition from:dim1-local-part.xml failed");
      retCode = 1;
    } else {
      System.out.println("Added partition from:dim1-local-part.xml");
    }
  }
}
