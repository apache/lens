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
package org.apache.lens.driver.jdbc;

import static org.apache.lens.driver.jdbc.JDBCDriverConfConstants.ConnectionPoolProperties.JDBC_GET_CONNECTION_TIMEOUT;
import static org.apache.lens.driver.jdbc.JDBCDriverConfConstants.ConnectionPoolProperties.JDBC_POOL_MAX_SIZE;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;

import org.testng.Assert;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;

/**
 * The Class TestDataSourceConnectionProvider.
 */
@Slf4j
public class TestDataSourceConnectionProvider {

  /**
   * Test get connection hsql.
   *
   * @throws Exception the exception
   */
  @Test
  public void testGetConnectionHSQL() throws Exception {
    final Configuration conf = new Configuration();
    conf.set(JDBCDriverConfConstants.JDBC_DRIVER_CLASS, "org.hsqldb.jdbc.JDBCDriver");
    conf.set(JDBCDriverConfConstants.JDBC_DB_URI, "jdbc:hsqldb:mem:mymemdb");
    conf.set(JDBCDriverConfConstants.JDBC_USER, "SA");
    conf.set(JDBCDriverConfConstants.JDBC_PASSWORD, "");
    final DataSourceConnectionProvider cp = new DataSourceConnectionProvider();

    int numThreads = 50;
    Thread[] threads = new Thread[numThreads];
    final AtomicInteger passed = new AtomicInteger(0);
    final Semaphore sem = new Semaphore(1);

    for (int i = 0; i < numThreads; i++) {
      final int thid = i;
      threads[thid] = new Thread(new Runnable() {
        @Override
        public void run() {
          Connection conn = null;
          Statement st = null;
          try {
            conn = cp.getConnection(conf);
            Assert.assertNotNull(conn);
            // Make sure the connection is usable
            st = conn.createStatement();
            Assert.assertNotNull(st);
            passed.incrementAndGet();
          } catch (SQLException e) {
            log.error("error getting connection to db!", e);
          } finally {
            if (st != null) {
              try {
                st.close();
              } catch (SQLException e) {
                log.error("Encountered SQL ecxception", e);
              }
            }
            if (conn != null) {
              try {
                conn.close();
              } catch (SQLException e) {
                log.error("Encountered SQL exception", e);
              }
            }
          }
        }
      });
      threads[thid].start();
    }

    for (Thread t : threads) {
      t.join();
    }
    cp.close();
    Assert.assertEquals(passed.get(), numThreads);
  }

  /**
   * Test get connection timeout.
   *
   * @throws Exception the exception
   */
  @Test
  public void testGetConnectionTimeout() throws Exception {
    final Configuration conf = new Configuration();
    conf.set(JDBCDriverConfConstants.JDBC_DRIVER_CLASS, "org.hsqldb.jdbc.JDBCDriver");
    conf.set(JDBCDriverConfConstants.JDBC_DB_URI, "jdbc:hsqldb:mem:mymemdb2");
    conf.set(JDBCDriverConfConstants.JDBC_USER, "SA");
    conf.set(JDBCDriverConfConstants.JDBC_PASSWORD, "");

    // Set a low timeout
    conf.setInt(JDBC_GET_CONNECTION_TIMEOUT.getConfigKey(), 1000);
    final int MAX_CONNECTIONS = 3;

    conf.setInt(JDBC_POOL_MAX_SIZE.getConfigKey(), MAX_CONNECTIONS);
    final DataSourceConnectionProvider cp = new DataSourceConnectionProvider();

    Connection[] connections = new Connection[MAX_CONNECTIONS + 1];
    for (int i = 0; i < MAX_CONNECTIONS + 1; i++) {
      // Last call should throw sql exception
      try {
        connections[i] = cp.getConnection(conf);
        if (i == MAX_CONNECTIONS) {
          fail("Expected last get connection to fail because of timeout");
        }
      } catch (SQLException sqlEx) {
        if (i != MAX_CONNECTIONS) {
          log.error("Unexpected getConnection error", sqlEx);
        }
        assertEquals(i, MAX_CONNECTIONS, "Failed before last getConnection call: " + sqlEx.getMessage());
      }
    }

    for (Connection c : connections) {
      if (c != null) {
        c.close();
      }
    }
  }
}
