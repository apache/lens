package com.inmobi.grill.driver.jdbc;

/*
 * #%L
 * Grill Driver for JDBC
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.ResultRow;
import com.inmobi.grill.server.api.driver.DriverQueryStatus.DriverQueryState;
import com.inmobi.grill.server.api.driver.GrillResultSet;
import com.inmobi.grill.server.api.driver.GrillResultSetMetadata;
import com.inmobi.grill.server.api.driver.InMemoryResultSet;
import com.inmobi.grill.server.api.driver.QueryCompletionListener;
import com.inmobi.grill.server.api.query.PreparedQueryContext;
import com.inmobi.grill.server.api.query.QueryContext;

import static org.testng.Assert.*;

public class TestJdbcDriver {
  Configuration baseConf;
  JDBCDriver driver;
  
  @BeforeTest
  public void testCreateJdbcDriver() throws Exception {
    baseConf = new Configuration();
    baseConf.set(JDBCDriverConfConstants.JDBC_DRIVER_CLASS, "org.hsqldb.jdbc.JDBCDriver");
    baseConf.set(JDBCDriverConfConstants.JDBC_DB_URI, "jdbc:hsqldb:mem:jdbcTestDB");
    baseConf.set(JDBCDriverConfConstants.JDBC_USER, "SA");
    baseConf.set(JDBCDriverConfConstants.JDBC_PASSWORD, "");
    baseConf.set(JDBCDriverConfConstants.JDBC_EXPLAIN_KEYWORD_PARAM, "explain plan for ");
    
    driver = new JDBCDriver();
    driver.configure(baseConf);
    assertNotNull(driver);
    assertTrue(driver.configured);
  }
  
  
  @AfterTest
  public void close() throws Exception {
    driver.close();
  }

  synchronized void createTable(String table) throws Exception {
    Connection conn = null;
    Statement stmt = null;
    try {
      conn = driver.getConnection(baseConf);
      stmt = conn.createStatement();
      stmt.execute("CREATE TABLE " + table + " (ID INT)");
      
      conn.commit();
    } finally {
      if (stmt != null) {
        stmt.close();
      }
      if (conn != null) {
       conn.close();
      }
    }
  }
  
  void insertData(String table) throws Exception {
    Connection conn = null;
    PreparedStatement stmt = null;
    try {
      conn = driver.getConnection(baseConf);
      stmt = conn.prepareStatement("INSERT INTO " + table + " VALUES(?)");
      
      for (int i = 0; i < 10; i ++) {
        stmt.setInt(1, i);
        stmt.executeUpdate();
      }
      
      conn.commit();
    } finally {
      if (stmt != null) {
        stmt.close();
      }
      if (conn != null) {
       conn.close();
      }
    }
  }

  @Test
  public void testDDLQueries() {
    String query = "DROP TABLE TEMP";

    Throwable th = null;
    try {
      driver.rewriteQuery(query, baseConf);
    } catch (GrillException e) {
      e.printStackTrace();
      th = e;
    }
    Assert.assertNotNull(th);

    query = "create table temp(name string, msr int)";

    th = null;
    try {
      driver.rewriteQuery(query, baseConf);
    } catch (GrillException e) {
      e.printStackTrace();
      th = e;
    }
    Assert.assertNotNull(th);

    query = "insert overwrite table temp SELECT * FROM execute_test";

    th = null;
    try {
      driver.rewriteQuery(query, baseConf);
    } catch (GrillException e) {
      e.printStackTrace();
      th = e;
    }
    Assert.assertNotNull(th);

    query = "create table temp2 as SELECT * FROM execute_test";

    th = null;
    try {
      driver.rewriteQuery(query, baseConf);
    } catch (GrillException e) {
      e.printStackTrace();
      th = e;
    }
    Assert.assertNotNull(th);
  }
  
  @Test
  public void testExplain() throws Exception {
    createTable("explain_test"); // Create table
    insertData("explain_test");  // Insert some data into table
    String query1 = "SELECT * FROM explain_test"; // Select query against existing table
    String query2 = "SELECT * FROM explain_test1"; // Select query against non existing table
    driver.explain(query1,baseConf);

    try {
    driver.explain(query2,baseConf); 
    Assert.fail("Running explain on a non existing table.");
    } catch(GrillException ex) {
      System.out.println("Error : " + ex);
    }
 }
  
  @Test
  public void testExecute() throws Exception {
    createTable("execute_test");
    
    // Insert some data into table
    insertData("execute_test");
    
    // Query
    String query = "SELECT * FROM execute_test";
    
    QueryContext context = new QueryContext(query, "SA", baseConf);
    GrillResultSet resultSet = driver.execute(context);
    assertNotNull(resultSet);
    if (resultSet instanceof InMemoryResultSet) {
      InMemoryResultSet rs = (InMemoryResultSet) resultSet;
      GrillResultSetMetadata rsMeta = rs.getMetadata();
      assertEquals(rsMeta.getColumns().size(), 1);
      
      ColumnDescriptor col1 = rsMeta.getColumns().get(0);
      assertEquals(col1.getTypeName().toLowerCase(), "int");
      assertEquals(col1.getName(), "ID");
      
      while (rs.hasNext()) {
        ResultRow row = rs.next();
        List<Object> rowObjects = row.getValues();
      }
      
      if (rs instanceof JDBCResultSet) {
        ((JDBCResultSet) rs).close();
      }
    }
  }
  
  @Test
  public void testPrepare() throws Exception {
    createTable("prepare_test");
    insertData("prepare_test");
    
    String query = "SELECT * from prepare_test";
    PreparedQueryContext pContext = new PreparedQueryContext(query, "SA", baseConf);
    driver.prepare(pContext);
  }
  
  @Test
  public void testExecuteAsync() throws Exception {
    createTable("execute_async_test");
    insertData("execute_async_test");
    String query = "SELECT * FROM execute_async_test";
    QueryContext context = new QueryContext(query, "SA", baseConf);
    System.out.println("@@@ Test_execute_async:" + context.getQueryHandle());
    final CountDownLatch listenerNotificationLatch = new CountDownLatch(1); 

    QueryCompletionListener listener = new QueryCompletionListener() {
      @Override
      public void onError(QueryHandle handle, String error) {
        fail("Query failed " + handle + " message" + error);
      }
      
      @Override
      public void onCompletion(QueryHandle handle) {
        System.out.println("@@@@ Query is complete " + handle);
        listenerNotificationLatch.countDown();
      }
    };
    
    driver.executeAsync(context);
    QueryHandle handle = context.getQueryHandle();
    driver.registerForCompletionNotification(handle, 0, listener);
    
    while(true) {
      driver.updateStatus(context);
      System.out.println("Query: " + handle + " Status: " + context.getDriverStatus());
      if (context.getDriverStatus().isFinished()) {
        assertEquals(context.getDriverStatus().getState(), DriverQueryState.SUCCESSFUL);
        assertEquals(context.getDriverStatus().getProgress(), 1.0);
        break;
      }
      Thread.sleep(500);
    }
    assertTrue(context.getDriverStatus().getDriverStartTime() > 0);
    assertTrue(context.getDriverStatus().getDriverFinishTime() > 0);
    // make sure query completion listener was called with onCompletion
    try {
      listenerNotificationLatch.await(1, TimeUnit.SECONDS);
    } catch (Exception e) {
      fail("query completion listener was not notified - " + e.getMessage());
      e.printStackTrace();
    }
    
    GrillResultSet grs = driver.fetchResultSet(context);
    
    // Check multiple fetchResultSet return same object
    for (int i = 0; i < 5; i++) {
      assertTrue(grs == driver.fetchResultSet(context));
    }
    
    assertNotNull(grs);
    if (grs instanceof InMemoryResultSet) {
      InMemoryResultSet rs = (InMemoryResultSet) grs;
      GrillResultSetMetadata rsMeta = rs.getMetadata();
      assertEquals(rsMeta.getColumns().size(), 1);
      
      ColumnDescriptor col1 = rsMeta.getColumns().get(0);
      assertEquals(col1.getTypeName().toLowerCase(), "int");
      assertEquals(col1.getName(), "ID");
      System.out.println("Matched metadata");
      
      while (rs.hasNext()) {
        List<Object> vals = rs.next().getValues();
        assertEquals(vals.size(), 1);
        assertEquals(vals.get(0).getClass(), Integer.class);
      }
      
      driver.closeQuery(handle);
      // Close again, should get not found
      try {
        driver.closeQuery(handle);
        fail("Close again should have thrown exception");
      } catch (GrillException ex) {
        assertTrue(ex.getMessage().contains("not found") 
            && ex.getMessage().contains(handle.getHandleId().toString()));
        System.out.println("Matched exception");
      }
    } else {
      fail("Only in memory result set is supported as of now");
    }
    
  }

  @Test
  public void testConnectionCloseForFailedQueries() throws Exception {
    createTable("invalid_conn_close");
    insertData("invalid_conn_close");

    String query = "SELECT * from invalid_conn_close2";
    QueryContext ctx = new QueryContext(query, "SA", baseConf);

    for (int i = 0; i < JDBCDriverConfConstants.JDBC_POOL_MAX_SIZE_DEFAULT; i++) {
      driver.executeAsync(ctx);
      driver.updateStatus(ctx);
      System.out.println("@@@@ QUERY " + (i+1));
    }

    String validQuery = "SELECT * FROM invalid_conn_close";
    QueryContext validCtx = new QueryContext(validQuery, "SA", baseConf);
    System.out.println("@@@ Submitting valid query");
    driver.executeAsync(validCtx);
    
    // Wait for query to finish
    while (true) {
      driver.updateStatus(validCtx);
      if (validCtx.getDriverStatus().isFinished()) {  
        break;
      }
      Thread.sleep(1000);
    }

    driver.closeQuery(validCtx.getQueryHandle());
  }
  
  @Test
  public void testConnectionCloseForSuccessfulQueries() throws Exception {
    createTable("valid_conn_close");
    insertData("valid_conn_close");

    String query = "SELECT * from valid_conn_close";
    QueryContext ctx = new QueryContext(query, "SA", baseConf);

    for (int i = 0; i < JDBCDriverConfConstants.JDBC_POOL_MAX_SIZE_DEFAULT; i++) {
     GrillResultSet resultSet = driver.execute(ctx);
     assertNotNull(resultSet);
     if (resultSet instanceof InMemoryResultSet) {
       InMemoryResultSet rs = (InMemoryResultSet) resultSet;
       GrillResultSetMetadata rsMeta = rs.getMetadata();
       assertEquals(rsMeta.getColumns().size(), 1);
       
       ColumnDescriptor col1 = rsMeta.getColumns().get(0);
       assertEquals(col1.getTypeName().toLowerCase(), "int");
       assertEquals(col1.getName(), "ID");
      
       while (rs.hasNext()) {
         ResultRow row = rs.next();
         List<Object> rowObjects = row.getValues();
       }
     }
      System.out.println("@@@@ QUERY " + (i+1));
    } 
    
    String validQuery = "SELECT * FROM valid_conn_close";
    QueryContext validCtx = new QueryContext(validQuery, "SA", baseConf);
    System.out.println("@@@ Submitting query after pool quota used");
    driver.execute(validCtx); 
 }
  
  @Test
  public void testCancelQuery() throws Exception {
    createTable("cancel_query_test");
    insertData("cancel_query_test");
    String query = "SELECT * FROM cancel_query_test";
    QueryContext context = new QueryContext(query, "SA", baseConf);
    System.out.println("@@@ test_cancel:" + context.getQueryHandle());
    driver.executeAsync(context);
    QueryHandle handle = context.getQueryHandle();
    driver.cancelQuery(handle);
    driver.updateStatus(context);
    assertEquals(context.getDriverStatus().getState(), DriverQueryState.CANCELED);
    assertTrue(context.getDriverStatus().getDriverStartTime() > 0);
    assertTrue(context.getDriverStatus().getDriverFinishTime() > 0);
    driver.closeQuery(handle);
  }

  @Test
  public void testInvalidQuery() throws Exception {
    String query = "SELECT * FROM invalid_table";
    QueryContext ctx = new QueryContext(query, "SA", baseConf);
    try {
      GrillResultSet rs = driver.execute(ctx);
      fail("Should have thrown exception");
    } catch (GrillException e) {
      e.printStackTrace();
    }
    
    final CountDownLatch listenerNotificationLatch = new CountDownLatch(1); 

    QueryCompletionListener listener = new QueryCompletionListener() {
      @Override
      public void onError(QueryHandle handle, String error) {
        listenerNotificationLatch.countDown();
      }
      
      @Override
      public void onCompletion(QueryHandle handle) {
        fail("Was expecting this query to fail " + handle);
      }
    };
    
    driver.executeAsync(ctx);
    QueryHandle handle = ctx.getQueryHandle();
    driver.registerForCompletionNotification(handle, 0, listener);
   
    while(true) {
      driver.updateStatus(ctx);
      System.out.println("Query: " + handle + " Status: " + ctx.getDriverStatus());
      if (ctx.getDriverStatus().isFinished()) {
        assertEquals(ctx.getDriverStatus().getState(), DriverQueryState.FAILED);
        assertEquals(ctx.getDriverStatus().getProgress(), 1.0);
        break;
      }
      Thread.sleep(500);
    }
    assertTrue(ctx.getDriverStatus().getDriverStartTime() > 0);
    assertTrue(ctx.getDriverStatus().getDriverFinishTime() > 0);
    
    listenerNotificationLatch.await(1, TimeUnit.SECONDS);
    // fetch result should throw error
    try {
      driver.fetchResultSet(ctx);
      fail("should have thrown error");
    } catch (GrillException e) {
      e.printStackTrace();
    }
    driver.closeQuery(handle);
  }

}
