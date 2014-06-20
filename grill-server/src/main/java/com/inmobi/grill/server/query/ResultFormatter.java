package com.inmobi.grill.server.query;

/*
 * #%L
 * Grill Server
 * %%
 * Copyright (C) 2014 Inmobi
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.driver.GrillResultSet;
import com.inmobi.grill.server.api.driver.InMemoryResultSet;
import com.inmobi.grill.server.api.driver.PersistentResultSet;
import com.inmobi.grill.server.api.events.AsyncEventListener;
import com.inmobi.grill.server.api.query.PersistedOutputFormatter;
import com.inmobi.grill.server.api.query.QueryContext;
import com.inmobi.grill.server.api.query.InMemoryOutputFormatter;
import com.inmobi.grill.server.api.query.QueryExecuted;
import com.inmobi.grill.server.api.query.QueryOutputFormatter;

public class ResultFormatter extends AsyncEventListener<QueryExecuted> {

  public static final Log LOG = LogFactory.getLog(ResultFormatter.class);
  QueryExecutionServiceImpl queryService;
  public ResultFormatter(QueryExecutionServiceImpl queryService) {
    this.queryService = queryService;
  }

  @Override
  public void process(QueryExecuted event) {
    formatOutput(event);    
  }

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
        GrillResultSet resultSet = queryService.getDriverResultset(queryHandle);
        if (resultSet instanceof PersistentResultSet) {
          // skip result formatting if persisted size is huge
          Path persistedDirectory = new Path(ctx.getHdfsoutPath());
          FileSystem fs = persistedDirectory.getFileSystem(ctx.getConf());
          long size = fs.getContentSummary(persistedDirectory).getLength();
          long threshold = ctx.getConf().getLong(
              GrillConfConstants.RESULT_FORMAT_SIZE_THRESHOLD,
              GrillConfConstants.DEFAULT_RESULT_FORMAT_SIZE_THRESHOLD);
          LOG.info(" size :" + size + " threshold:" + threshold);
          if (size > threshold) {
            LOG.warn("Persisted result size more than the threshold, size:" +
              size + " and threshold:" + threshold + "; Skipping formatter");
            queryService.setSuccessState(ctx);
            return;
          }
        }
        // now do the formatting
        createAndSetFormatter(ctx);
        QueryOutputFormatter formatter = ctx.getQueryOutputFormatter();
        try {
          formatter.init(ctx, resultSet.getMetadata());
          if (ctx.getConf().getBoolean(GrillConfConstants.QUERY_OUTPUT_WRITE_HEADER,
              GrillConfConstants.DEFAULT_OUTPUT_WRITE_HEADER)) {
            formatter.writeHeader();
          }
          if (resultSet instanceof PersistentResultSet) {
            LOG.info("Result formatter for " + queryHandle + " in persistent result");
            Path persistedDirectory = new Path(ctx.getHdfsoutPath());
            //write all files from persistent directory
            ((PersistedOutputFormatter)formatter).addRowsFromPersistedPath(persistedDirectory);
          } else {
            LOG.info("Result formatter for " + queryHandle + " in inmemory result");
            InMemoryResultSet inmemory = (InMemoryResultSet)resultSet;
            while (inmemory.hasNext()) {
              ((InMemoryOutputFormatter)formatter).writeRow(inmemory.next());
            }
          }
          if (ctx.getConf().getBoolean(GrillConfConstants.QUERY_OUTPUT_WRITE_FOOTER,
              GrillConfConstants.DEFAULT_OUTPUT_WRITE_FOOTER)) {
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
      LOG.warn("Exception while formatting result for " + queryHandle, e);
      try {
        queryService.setFailedStatus(ctx, "Result formatting failed!", e.getLocalizedMessage());
      } catch (GrillException e1) {
        LOG.error("Exception while setting failure for " + queryHandle, e1);
      }
    }
  }

  @SuppressWarnings("unchecked")
  void createAndSetFormatter(QueryContext ctx) throws GrillException {
    if (ctx.getQueryOutputFormatter() == null && ctx.isPersistent()) {
      QueryOutputFormatter formatter;
      try {
        if (ctx.isDriverPersistent()) {
          formatter = ReflectionUtils.newInstance(
              ctx.getConf().getClass(
                  GrillConfConstants.QUERY_OUTPUT_FORMATTER,
                  (Class<? extends PersistedOutputFormatter>)Class.forName(
                      GrillConfConstants.DEFAULT_PERSISTENT_OUTPUT_FORMATTER),
                      PersistedOutputFormatter.class), ctx.getConf());
        } else {
          formatter = ReflectionUtils.newInstance(
              ctx.getConf().getClass(
                  GrillConfConstants.QUERY_OUTPUT_FORMATTER,
                  (Class<? extends InMemoryOutputFormatter>)Class.forName(
                      GrillConfConstants.DEFAULT_INMEMORY_OUTPUT_FORMATTER),
                      InMemoryOutputFormatter.class), ctx.getConf()); 
        }
      } catch (ClassNotFoundException e) {
        throw new GrillException(e);
      }
      ctx.setQueryOutputFormatter(formatter);
    }
  }

}
