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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import javax.xml.bind.JAXBException;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.query.*;
import org.apache.lens.client.LensClient;
import org.apache.lens.client.LensClientSingletonWrapper;
import org.apache.lens.client.LensMetadataClient;
import org.apache.lens.client.LensStatement;
import org.apache.lens.client.exceptions.LensAPIException;

import org.apache.commons.lang.StringUtils;

/**
 * The Class SampleQueries.
 */
public class SampleQueries {

  /** The meta client. */
  private LensMetadataClient metaClient;

  /** The query client. */
  private LensStatement queryClient;

  /** The ret code. */
  private int retCode = 0;

  /**
   * Instantiates a new sample queries.
   *
   * @throws JAXBException the JAXB exception
   */
  public SampleQueries() throws JAXBException {
    metaClient = new LensMetadataClient(LensClientSingletonWrapper.instance().getClient().getConnection());
    queryClient = new LensStatement(LensClientSingletonWrapper.instance().getClient().getConnection());
  }

  /**
   * Close.
   */
  public void close() {
    LensClientSingletonWrapper.instance().getClient().closeConnection();
  }

  /**
   * The main method.
   *
   * @param args the arguments
   * @throws Exception the exception
   */
  public static void main(String[] args) throws Exception {
    SampleQueries queries = null;
    long start = System.currentTimeMillis();
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
    } finally {
      if (queries != null) {
        queries.close();
      }
      long end = System.currentTimeMillis();
      System.out.println("Total time for running examples(in millis) :" + (end-start));
    }
    if (queries.retCode != 0) {
      System.exit(queries.retCode);
    }
  }

  /**
   * Query all.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void queryAll() throws IOException, LensAPIException {
    runQueries("dimension-queries.sql");
    runQueries("cube-queries.sql");
    System.out.println("Successful queries " + success + " out of " + total + "queries");
  }

  /** The total. */
  int total = 0;

  /** The success. */
  int success = 0;

  /**
   * Run queries.
   *
   * @param fileName the file name
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void runQueries(String fileName) throws IOException, LensAPIException {
    InputStream file = SampleMetastore.class.getClassLoader().getResourceAsStream(fileName);
    BufferedReader reader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
    String query;
    while ((query = reader.readLine()) != null) {
      if (StringUtils.isBlank(query)) {
        continue;
      }
      if (query.startsWith("--")) {
        // skip comments
        continue;
      }
      total++;
      System.out.println("Query:" + query);
      try {
        QueryHandle handle = queryClient.executeQuery(query, true, null, new LensConf());
        System.out.println("Status:" + queryClient.getQuery().getStatus());
        System.out.println("Total time in millis:"
          + (queryClient.getQuery().getFinishTime() - queryClient.getQuery().getSubmissionTime()));
        System.out.println("Driver run time in millis:"
          + (queryClient.getQuery().getDriverFinishTime() - queryClient.getQuery().getDriverStartTime()));
        if (queryClient.wasQuerySuccessful()) {
          success++;
          if (queryClient.getQuery().getStatus().isResultSetAvailable()) {
            System.out.println("Result:");
            QueryResult queryResult = queryClient.getResultSet();
            if (queryResult instanceof InMemoryQueryResult) {
              InMemoryQueryResult result = (InMemoryQueryResult) queryResult;
              for (ResultRow row : result.getRows()) {
                System.out.println(StringUtils.join(row.getValues(), "\t"));
              }
            } else if (queryResult instanceof PersistentQueryResult) {
              PersistentQueryResult persistentQueryResult = (PersistentQueryResult) queryResult;
              System.out.println("Result stored at " + persistentQueryResult.getPersistedURI());
            }
            queryClient.closeResultSet();
          }
        } else {
          retCode = 1;
        }
      } catch (Exception e) {
        LensClient.getCliLogger().error("Exception for example query : \"{}\"", query, e);
        retCode = 1;
      }
      System.out.println("--------------------");
    }

  }

}
