package com.inmobi.grill.examples;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class DatabaseUtil {

  public static void initalizeDatabaseStorage() throws Exception {

    try {
      Class.forName("org.hsqldb.jdbcDriver");
    } catch (ClassNotFoundException e) {
      System.err.println("Unable to locate the JDBC driver class for DB Storage");
    }
    Connection con = DriverManager.getConnection(
        "jdbc:hsqldb:/tmp/db-storage.db", "SA", "");

    con.setAutoCommit(true);
    Statement statement = con.createStatement();

    InputStream file = DatabaseUtil.class.getClassLoader().getResourceAsStream("db-storage-schema.sql");
    BufferedReader reader = new BufferedReader(new InputStreamReader(file));
    String line;
    while((line = reader.readLine()) != null) {
      if(line.trim().equals("")){
        continue;
      }
      statement.executeUpdate(line);
    }
    statement.execute("SHUTDOWN");
    statement.close();
    con.close();
  }

  public static void main(String[] args) throws Exception {
    DatabaseUtil.initalizeDatabaseStorage();
  }
}
