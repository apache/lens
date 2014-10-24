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
package org.apache.lens.server.query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.lens.api.LensException;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.LensResultSet;
import org.apache.lens.server.api.driver.InMemoryResultSet;
import org.apache.lens.server.api.driver.PersistentResultSet;
import org.apache.lens.server.api.events.AsyncEventListener;
import org.apache.lens.server.api.metrics.MetricsService;
import org.apache.lens.server.api.query.InMemoryOutputFormatter;
import org.apache.lens.server.api.query.PersistedOutputFormatter;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.QueryExecuted;
import org.apache.lens.server.api.query.QueryOutputFormatter;

/**
 * The Class ResultFormatter.
 */
public class ResultFormatter extends AsyncEventListener<QueryExecuted> {

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(ResultFormatter.class);

  /** The query service. */
  QueryExecutionServiceImpl queryService;

  /**
   * Instantiates a new result formatter.
   *
   * @param queryService
   *          the query service
   */
  public ResultFormatter(QueryExecutionServiceImpl queryService) {
    this.queryService = queryService;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.events.AsyncEventListener#process(org.apache.lens.server.api.events.LensEvent)
   */
  @Override
  public void process(QueryExecuted event) {
    formatOutput(event);
  }

  /**
   * Format output.
   *
   * @param event
   *          the event
   */
  private void formatOutput(QueryExecuted event) {
    QueryHandle queryHandle = event.getQueryHandle();
    QueryContext ctx = queryService.getQueryContext(queryHandle);
    try {
      if (!ctx.isPersistent()) {
        LOG.info("No result formatting required for query " + queryHandle);
        return;
      }
      if (ctx.isResultAvailableInDriver()) {
        LOG.info("Result formatter for " + queryHandle);
        LensResultSet resultSet = queryService.getDriverResultset(queryHandle);
        if (resultSet instanceof PersistentResultSet) {
          // skip result formatting if persisted size is huge
          Path persistedDirectory = new Path(ctx.getHdfsoutPath());
          FileSystem fs = persistedDirectory.getFileSystem(ctx.getConf());
          long size = fs.getContentSummary(persistedDirectory).getLength();
          long threshold = ctx.getConf().getLong(LensConfConstants.RESULT_FORMAT_SIZE_THRESHOLD,
              LensConfConstants.DEFAULT_RESULT_FORMAT_SIZE_THRESHOLD);
          LOG.info(" size :" + size + " threshold:" + threshold);
          if (size > threshold) {
            LOG.warn("Persisted result size more than the threshold, size:" + size + " and threshold:" + threshold
                + "; Skipping formatter");
            queryService.setSuccessState(ctx);
            return;
          }
        }
        // now do the formatting
        createAndSetFormatter(ctx);
        QueryOutputFormatter formatter = ctx.getQueryOutputFormatter();
        try {
          formatter.init(ctx, resultSet.getMetadata());
          if (ctx.getConf().getBoolean(LensConfConstants.QUERY_OUTPUT_WRITE_HEADER,
              LensConfConstants.DEFAULT_OUTPUT_WRITE_HEADER)) {
            formatter.writeHeader();
          }
          if (resultSet instanceof PersistentResultSet) {
            LOG.info("Result formatter for " + queryHandle + " in persistent result");
            Path persistedDirectory = new Path(ctx.getHdfsoutPath());
            // write all files from persistent directory
            ((PersistedOutputFormatter) formatter).addRowsFromPersistedPath(persistedDirectory);
          } else {
            LOG.info("Result formatter for " + queryHandle + " in inmemory result");
            InMemoryResultSet inmemory = (InMemoryResultSet) resultSet;
            while (inmemory.hasNext()) {
              ((InMemoryOutputFormatter) formatter).writeRow(inmemory.next());
            }
          }
          if (ctx.getConf().getBoolean(LensConfConstants.QUERY_OUTPUT_WRITE_FOOTER,
              LensConfConstants.DEFAULT_OUTPUT_WRITE_FOOTER)) {
            formatter.writeFooter();
          }
          formatter.commit();
        } finally {
          formatter.close();
        }
        queryService.setSuccessState(ctx);
        LOG.info("Result formatter has completed. Final path:" + formatter.getFinalOutputPath());
      }
    } catch (Exception e) {
      MetricsService metricsService = (MetricsService) LensServices.get().getService(MetricsService.NAME);
      metricsService.incrCounter(ResultFormatter.class, "formatting-errors");
      LOG.warn("Exception while formatting result for " + queryHandle, e);
      try {
        queryService.setFailedStatus(ctx, "Result formatting failed!", e.getLocalizedMessage());
      } catch (LensException e1) {
        LOG.error("Exception while setting failure for " + queryHandle, e1);
      }
    }
  }

  /**
   * Creates the and set formatter.
   *
   * @param ctx
   *          the ctx
   * @throws LensException
   *           the lens exception
   */
  @SuppressWarnings("unchecked")
  void createAndSetFormatter(QueryContext ctx) throws LensException {
    if (ctx.getQueryOutputFormatter() == null && ctx.isPersistent()) {
      QueryOutputFormatter formatter;
      try {
        if (ctx.isDriverPersistent()) {
          formatter = ReflectionUtils.newInstance(
              ctx.getConf().getClass(
                  LensConfConstants.QUERY_OUTPUT_FORMATTER,
                  (Class<? extends PersistedOutputFormatter>) Class
                      .forName(LensConfConstants.DEFAULT_PERSISTENT_OUTPUT_FORMATTER), PersistedOutputFormatter.class),
              ctx.getConf());
        } else {
          formatter = ReflectionUtils.newInstance(
              ctx.getConf().getClass(
                  LensConfConstants.QUERY_OUTPUT_FORMATTER,
                  (Class<? extends InMemoryOutputFormatter>) Class
                      .forName(LensConfConstants.DEFAULT_INMEMORY_OUTPUT_FORMATTER), InMemoryOutputFormatter.class),
              ctx.getConf());
        }
      } catch (ClassNotFoundException e) {
        throw new LensException(e);
      }
      LOG.info("Created result formatter:" + formatter.getClass().getCanonicalName());
      ctx.setQueryOutputFormatter(formatter);
    }
  }

}
