package com.inmobi.grill.driver.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDataSourceConnectionProvider {
  public static final Logger LOG = Logger.getLogger(TestDataSourceConnectionProvider.class);

  @Test
  public void testGetConnectionHSQL() throws Exception {
    final Configuration conf = new Configuration();
    conf.set(JDBCDriverConfConstants.JDBC_DRIVER_CLASS, "org.hsqldb.jdbc.JDBCDriver");
    conf.set(JDBCDriverConfConstants.JDBC_DB_URI, "jdbc:hsqldb:mem:mymemdb");
    conf.set(JDBCDriverConfConstants.JDBC_USER, "SA");
    conf.set(JDBCDriverConfConstants.JDBC_PASSWORD, "");
    final DataSourceConnectionProvider cp = new DataSourceConnectionProvider();

    int numThreads = 50;
    Thread threads[] = new Thread[numThreads];
    final AtomicInteger passed = new AtomicInteger(0);
    final Semaphore sem = new Semaphore(1);

    for (int i = 0; i < numThreads; i++) {
      final int thid = i;
      threads[thid] = new Thread(new Runnable() {
        @Override
        public void run() {
          Connection conn = null;
          Statement st = null;
          List<Integer> expected;
          List<Integer> actual;
          try {
            conn = cp.getConnection(conf);
            conn.setAutoCommit(false);

            sem.acquire();
            try {
              st = conn.createStatement();
              st.execute("CREATE TABLE TEST_" + thid + "(ID INT)");
              st.close();
              conn.commit();
            } finally {
              sem.release();
            }

            PreparedStatement ps = conn.prepareStatement("INSERT INTO TEST_" + thid + " VALUES(?)");
            expected = new ArrayList<Integer>();
            for (int j = 0; j < 10; j++) {
              ps.setInt(1, j);
              ps.executeUpdate();
              expected.add(j);
            }
            ps.close();
            conn.commit();

            st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM TEST_" + thid);
            actual = new ArrayList<Integer>();
            while (rs.next()) {
              actual.add(rs.getInt(1));
            }
            Assert.assertEquals(actual, expected);
            System.out.println("Test:" + thid + " passs");
            passed.incrementAndGet();
          } catch (SQLException e) {
            LOG.error("Error in test:" + thid, e);
          } catch (InterruptedException e1) {
            LOG.error("Interrupted:" + thid + " : " + e1.getMessage());
          } finally {
            try {
              st.close();
              conn.close();
            } catch (SQLException e) {
              e.printStackTrace();
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
}
