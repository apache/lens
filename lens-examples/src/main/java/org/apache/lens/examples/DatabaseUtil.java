package org.apache.lens.examples;

/*
 * #%L
 * Lens Examples
 * %%
 * Copyright (C) 2014 Apache Software Foundation
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
    while ((line = reader.readLine()) != null) {
      if (line.trim().equals("") || line.startsWith("--")) {
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
