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

import static org.apache.lens.server.api.LensConfConstants.*;

import java.io.IOException;
import java.util.Calendar;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.lens.cube.metadata.DateUtil;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.metrics.MetricsService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import lombok.extern.slf4j.Slf4j;


/**
 * The Class QueryResultPurger - Purges old files in query resultset directory and hdfs output directory.
 */
@Slf4j
public class QueryResultPurger implements Runnable {

  /**
   * The resultset retention
   */
  private DateUtil.TimeDiff resultsetRetention;

  /**
   * The hdfs output retention
   */
  private DateUtil.TimeDiff hdfsOutputRetention;

  /**
   * The query result purger executor
   */
  private ScheduledExecutorService queryResultPurgerExecutor;

  private Path resultsetPath;

  private Path hdfsOutputPath;

  private Configuration conf;

  /**
   * The Constant QUERY_RESULT_PURGER_COUNTER.
   */
  public static final String QUERY_RESULT_PURGER_ERROR_COUNTER = "query-result-purger-errors";

  /**
   * The metrics service.
   */
  private MetricsService metricsService;

  public void init(Configuration configuration) {
    this.conf = configuration;
    this.resultsetPath = new Path(conf.get(RESULT_SET_PARENT_DIR, RESULT_SET_PARENT_DIR_DEFAULT));
    this.hdfsOutputPath = new Path(resultsetPath.toString(),
      conf.get(QUERY_HDFS_OUTPUT_PATH, DEFAULT_HDFS_OUTPUT_PATH));
    int purgeDelay = conf.getInt(RESULTSET_PURGE_INTERVAL_IN_SECONDS, DEFAULT_RESULTSET_PURGE_INTERVAL_IN_SECONDS);

    try {
      String resultSetDiffStr = conf.get(QUERY_RESULTSET_RETENTION, DEFAULT_QUERY_RESULTSET_RETENTION);
      String hdfsOutputDiffStr = conf.get(QUERY_RESULTSET_RETENTION, DEFAULT_QUERY_RESULTSET_RETENTION);
      this.resultsetRetention = DateUtil.TimeDiff.parseFrom(resultSetDiffStr);
      this.hdfsOutputRetention = DateUtil.TimeDiff.parseFrom(hdfsOutputDiffStr);
      queryResultPurgerExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          return new Thread(r, "QueryResultPurger");
        }
      });
      queryResultPurgerExecutor.scheduleWithFixedDelay(this, purgeDelay,
        purgeDelay, TimeUnit.SECONDS);
      log.info(
        "Initialized query result purger with lens resultset retention of {} and hdfs output retention of {}, "
          + "scheduled to run every {} seconds",
        resultSetDiffStr, hdfsOutputDiffStr, purgeDelay);
    } catch (LensException e) {
      log.error("Error occurred while initializing query result purger", e);
    }
  }

  public void purgePaths(Path path, DateUtil.TimeDiff retention, boolean purgeDirectory) throws IOException {
    int counter = 0;
    FileSystem fs = path.getFileSystem(conf);
    FileStatus[] fileList = fs.listStatus(path);
    for (FileStatus f : fileList) {
      if ((f.isFile() || (f.isDirectory() && purgeDirectory)) && canBePurged(f, retention)) {
        try {
          if (fs.delete(f.getPath(), true)) {
            counter++;
          } else {
            getMetrics().incrCounter(this.getClass(), QUERY_RESULT_PURGER_ERROR_COUNTER);
          }
        } catch (IOException e) {
          getMetrics().incrCounter(this.getClass(), QUERY_RESULT_PURGER_ERROR_COUNTER);
        }
      }
    }
    log.info("Purged {} files/directories in {}", counter, path.toString());
  }

  @Override
  public void run() {
    try {
      purgePaths(resultsetPath, resultsetRetention, false);
      purgePaths(hdfsOutputPath, hdfsOutputRetention, true);
    } catch (Exception e) {
      log.error("Error occurred in Query result purger", e);
      getMetrics().incrCounter(this.getClass(), QUERY_RESULT_PURGER_ERROR_COUNTER);
    }
  }

  private boolean canBePurged(FileStatus f, DateUtil.TimeDiff retention) {
    return f.getModificationTime() < retention.negativeOffsetFrom(Calendar.getInstance().getTime()).getTime();
  }

  /**
   * Stops query result purger
   */
  public void stop() {
    if (null != queryResultPurgerExecutor) {
      queryResultPurgerExecutor.shutdownNow();
      log.info("Stopped query result purger.");
    }
  }

  /**
   * Checks the status of executor service
   *
   * @return
   */
  public boolean isHealthy() {
    if (null == queryResultPurgerExecutor || queryResultPurgerExecutor.isShutdown()
      || queryResultPurgerExecutor.isTerminated()) {
      return false;
    }
    return true;
  }

  private MetricsService getMetrics() {
    if (metricsService == null) {
      metricsService = LensServices.get().getService(MetricsService.NAME);
      if (metricsService == null) {
        throw new NullPointerException("Could not get metrics service");
      }
    }
    return metricsService;
  }
}
