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

import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.InMemoryResultSet;
import org.apache.lens.server.api.driver.LensResultSet;
import org.apache.lens.server.api.driver.PersistentResultSet;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.events.AsyncEventListener;
import org.apache.lens.server.api.metrics.MetricsService;
import org.apache.lens.server.api.query.*;
import org.apache.lens.server.model.LogSegregationContext;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * The Class ResultFormatter.
 */
@Slf4j
public class ResultFormatter extends AsyncEventListener<QueryExecuted> {

  /** The query service. */
  QueryExecutionServiceImpl queryService;

  /** ResultFormatter core and max pool size */
  private static final int CORE_POOL_SIZE = 5;
  private static final int MAX_POOL_SIZE = 10;

  private final LogSegregationContext logSegregationContext;

  /**
   * Instantiates a new result formatter.
   *
   * @param queryService the query service
   */
  public ResultFormatter(QueryExecutionServiceImpl queryService, @NonNull LogSegregationContext logSegregationContext) {
    super(CORE_POOL_SIZE, MAX_POOL_SIZE);
    this.queryService = queryService;
    this.logSegregationContext = logSegregationContext;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.events.AsyncEventListener#process(org.apache.lens.server.api.events.LensEvent)
   */
  @Override
  public void process(QueryExecuted event) {
    formatOutput(queryService.getQueryContext(event.getQueryHandle()));
  }

  /**
   * Format output.
   *
   * @param ctx the query context
   */
  private void formatOutput(QueryContext ctx) {
    QueryHandle queryHandle = ctx.getQueryHandle();
    this.logSegregationContext.setLogSegragationAndQueryId(ctx.getQueryHandleString());
    try {
      if (!ctx.isPersistent()) {
        log.info("No result formatting required for query " + queryHandle);
        return;
      }
      if (ctx.isResultAvailableInDriver()) {
        log.info("Result formatter for {}", queryHandle);
        LensResultSet resultSet = queryService.getDriverResultset(queryHandle);
        boolean isPersistedInDriver = resultSet instanceof PersistentResultSet;
        if (isPersistedInDriver) {          // skip result formatting if persisted size is huge
          Path persistedDirectory = new Path(ctx.getDriverResultPath());
          FileSystem fs = persistedDirectory.getFileSystem(ctx.getConf());
          long size = fs.getContentSummary(persistedDirectory).getLength();
          long threshold = ctx.getConf().getLong(LensConfConstants.RESULT_FORMAT_SIZE_THRESHOLD,
            LensConfConstants.DEFAULT_RESULT_FORMAT_SIZE_THRESHOLD);
          log.info(" size :{} threshold:{}", size, threshold);
          if (size > threshold) {
            log.warn("Persisted result size more than the threshold, size:{} and threshold:{}; Skipping formatter",
              size, threshold);
            queryService.setSuccessState(ctx);
            return;
          }
        }
        // now do the formatting
        createAndSetFormatter(ctx, isPersistedInDriver);
        QueryOutputFormatter formatter = ctx.getQueryOutputFormatter();
        try {
          formatter.init(ctx, resultSet.getMetadata());
          if (ctx.getConf().getBoolean(LensConfConstants.QUERY_OUTPUT_WRITE_HEADER,
            LensConfConstants.DEFAULT_OUTPUT_WRITE_HEADER)) {
            formatter.writeHeader();
          }
          if (isPersistedInDriver) {
            log.info("Result formatter for {} in persistent result", queryHandle);
            Path persistedDirectory = new Path(ctx.getDriverResultPath());
            // write all files from persistent directory
            ((PersistedOutputFormatter) formatter).addRowsFromPersistedPath(persistedDirectory);
          } else {
            log.info("Result formatter for {} in inmemory result", queryHandle);
            InMemoryResultSet inmemory = (InMemoryResultSet) resultSet;
            while (inmemory.hasNext()) {
              ((InMemoryOutputFormatter) formatter).writeRow(inmemory.next());
            }
            inmemory.setFullyAccessed(true);
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
        log.info("Result formatter has completed. Final path:{}", formatter.getFinalOutputPath());
      }
    } catch (Exception e) {
      MetricsService metricsService = LensServices.get().getService(MetricsService.NAME);
      metricsService.incrCounter(ResultFormatter.class, "formatting-errors");
      log.warn("Exception while formatting result for {}", queryHandle, e);
      try {
        queryService.setFailedStatus(ctx, "Result formatting failed!", e.getMessage(), null);
      } catch (LensException e1) {
        log.error("Exception while setting failure for {}", queryHandle, e1);
      }
    }
  }

  /**
   * Creates the and set formatter.
   *
   * @param ctx                 the ctx
   * @param isPersistedInDriver
   * @throws LensException the lens exception
   */
  @SuppressWarnings("unchecked")
  void createAndSetFormatter(QueryContext ctx, boolean isPersistedInDriver) throws LensException {
    if (ctx.getQueryOutputFormatter() == null && ctx.isPersistent()) {
      QueryOutputFormatter formatter;
      try {
        if (isPersistedInDriver) {
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
      log.info("Created result formatter:{}", formatter.getClass().getCanonicalName());
      ctx.setQueryOutputFormatter(formatter);
    }
  }

}
