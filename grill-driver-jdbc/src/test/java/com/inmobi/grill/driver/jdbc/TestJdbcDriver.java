package com.inmobi.grill.driver.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.api.query.QueryStatus.Status;
import com.inmobi.grill.api.query.ResultColumn;
import com.inmobi.grill.api.query.ResultColumnType;
import com.inmobi.grill.api.query.ResultRow;
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
      
      ResultColumn col1 = rsMeta.getColumns().get(0);
      assertEquals(col1.getType(), ResultColumnType.INT);
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
    PreparedQueryContext pContext = new PreparedQueryContext(query, "SQ", baseConf);
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
      QueryStatus status = driver.getStatus(handle);
      System.out.println("Query: " + handle + " Status: " + status);
      if (status.isFinished()) {
        assertEquals(status.getStatus(), QueryStatus.Status.SUCCESSFUL);
        assertEquals(status.getProgress(), 1.0);
        break;
      }
      Thread.sleep(500);
    }
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
      
      ResultColumn col1 = rsMeta.getColumns().get(0);
      assertEquals(col1.getType(), ResultColumnType.INT);
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
  public void testCancelQuery() throws Exception {
    createTable("cancel_query_test");
    insertData("cancel_query_test");
    String query = "SELECT * FROM cancel_query_test";
    QueryContext context = new QueryContext(query, "SA", baseConf);
    System.out.println("@@@ test_cancel:" + context.getQueryHandle());
    driver.executeAsync(context);
    QueryHandle handle = context.getQueryHandle();
    driver.cancelQuery(handle);
    QueryStatus status = driver.getStatus(handle);
    assertEquals(status.getStatus(), Status.CANCELED);
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
      QueryStatus status = driver.getStatus(handle);
      System.out.println("Query: " + handle + " Status: " + status);
      if (status.isFinished()) {
        assertEquals(status.getStatus(), QueryStatus.Status.FAILED);
        assertEquals(status.getProgress(), 1.0);
        break;
      }
      Thread.sleep(500);
    }
    
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
