package com.inmobi.grill.driver.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.grill.api.query.ResultColumn;
import com.inmobi.grill.api.query.ResultColumnType;
import com.inmobi.grill.api.query.ResultRow;
import com.inmobi.grill.server.api.driver.GrillResultSet;
import com.inmobi.grill.server.api.driver.GrillResultSetMetadata;
import com.inmobi.grill.server.api.driver.InMemoryResultSet;
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
        JDBCResultSet jdbcResultSet = (JDBCResultSet) rs;
        assertTrue(jdbcResultSet.isClosed(), "Expected result set to be closed after consuming");
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

}
