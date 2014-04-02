package com.inmobi.grill.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import javax.xml.bind.JAXBException;

import org.apache.commons.lang.StringUtils;

import com.inmobi.grill.api.APIResult;
import com.inmobi.grill.api.query.InMemoryQueryResult;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.ResultRow;
import com.inmobi.grill.client.GrillConnection;
import com.inmobi.grill.client.GrillConnectionParams;
import com.inmobi.grill.client.GrillMetadataClient;
import com.inmobi.grill.client.GrillStatement;

public class SampleQueries {
  private GrillConnection connection;
  private GrillMetadataClient metaClient;
  private GrillStatement queryClient;
  private int retCode = 0;

  public SampleQueries() throws JAXBException {
    connection = new GrillConnection(new GrillConnectionParams());
    connection.open();
    metaClient = new GrillMetadataClient(connection);
    queryClient = new GrillStatement(connection);
  }

  public void close() {
    connection.close();
  }

  public static void main(String[] args) throws Exception {
    SampleQueries queries = null;
    try {
      queries = new SampleQueries();
      if (args.length > 0) {
        if (args[0].equals("-db")) {
          String dbName = args[1];
          queries.metaClient.createDatabase(dbName, true);
          queries.metaClient.setDatabase(dbName);
        }
      }
      queries.queryAll();
      if (queries.retCode != 0) {
        System.exit(queries.retCode);
      }
    }finally {
      if (queries != null) {
        queries.close();
      }
    }
  }

  public void queryAll() throws IOException {
    dimensionQueries();

  }

  public void dimensionQueries() throws IOException {
    InputStream file = SampleMetastore.class.getClassLoader().getResourceAsStream("dimension-queries.txt");
    BufferedReader reader = new BufferedReader(new InputStreamReader(file));
    String query;
    while ((query = reader.readLine()) != null) {
      if (StringUtils.isBlank(query)) {
        continue;
      }
      QueryHandle handle = queryClient.executeQuery(query, true);
      System.out.println("Query:" + query);
      System.out.println("Status:" + queryClient.getQuery().getStatus());
      if (queryClient.wasQuerySuccessful()) {
        System.out.println("Result:");
        InMemoryQueryResult result = queryClient.getResultSet();
        for (ResultRow row : result.getRows()) {
          System.out.println(StringUtils.join(row.getValues(), "\t"));
        }
        queryClient.closeResultSet();
      } else {
        System.out.println("query failed!");
        retCode = 1;
      }
    }

  }

}
