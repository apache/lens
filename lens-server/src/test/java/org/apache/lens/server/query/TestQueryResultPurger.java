/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lens.server.query;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.lens.server.api.LensConfConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@Test(groups = "unit-test")
public class TestQueryResultPurger {

  private Configuration conf;
  private static final long MILLISECONDS_IN_DAY = 24 * 60 * 60 * 1000;

  /**
   * Test query result purger
   *
   * @throws InterruptedException the interrupted exception
   * @throws IOException          Signals that an I/O exception has occurred.
   */

  @BeforeTest
  public void setUp() throws IOException {
    String resultsetPath = "target/" + getClass().getSimpleName();
    conf = new Configuration();
    conf.set(LensConfConstants.RESULT_SET_PARENT_DIR, resultsetPath);
    conf.set(LensConfConstants.QUERY_HDFS_OUTPUT_PATH, "hdfsout");
    conf.set(LensConfConstants.QUERY_RESULTSET_RETENTION, "1 day");
    conf.set(LensConfConstants.HDFS_OUTPUT_RETENTION, "1 day");
    conf.set(LensConfConstants.RESULTSET_PURGE_INTERVAL_IN_SECONDS, "1");
    createTestFiles();
  }

  @AfterTest
  public void cleanup() throws Exception {
    Path dir = new Path(conf.get(LensConfConstants.RESULT_SET_PARENT_DIR));
    FileSystem fs = dir.getFileSystem(conf);
    fs.delete(dir, true);
  }

  @Test
  public void testQueryResultPurger() throws InterruptedException, IOException {
    verify(conf.get(LensConfConstants.RESULT_SET_PARENT_DIR), 2);
    verify(conf.get(LensConfConstants.RESULT_SET_PARENT_DIR) + "/" + conf.get(LensConfConstants.QUERY_HDFS_OUTPUT_PATH),
      1);
    QueryResultPurger queryResultPurger = new QueryResultPurger();
    queryResultPurger.init(conf);
    Thread.sleep(2000); // sleep for 2 seconds, enough to run query purger
    queryResultPurger.stop();
    verify(conf.get(LensConfConstants.RESULT_SET_PARENT_DIR), 1);
    verify(conf.get(LensConfConstants.RESULT_SET_PARENT_DIR) + "/" + conf.get(LensConfConstants.QUERY_HDFS_OUTPUT_PATH),
      0);
  }

  private void verify(String path, int count) {
    File f = new File(path);
    assertEquals(f.list().length, count);
  }

  private void createTestFiles() throws IOException {
    long delta = 60 * 1000; //60 seconds
    long lastModified = System.currentTimeMillis() - (MILLISECONDS_IN_DAY + delta);
    File hdfsDir = new File(
      conf.get(LensConfConstants.RESULT_SET_PARENT_DIR) + "/" + conf.get(LensConfConstants.QUERY_HDFS_OUTPUT_PATH)
        + "/test-dir");
    hdfsDir.mkdirs();
    hdfsDir.setLastModified(lastModified);
    File resultFile = new File(conf.get(LensConfConstants.RESULT_SET_PARENT_DIR) + "/test-result.txt");
    resultFile.createNewFile();
    resultFile.setLastModified(lastModified);
  }
}
