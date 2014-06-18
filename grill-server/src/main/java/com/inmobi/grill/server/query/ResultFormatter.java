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
import org.apache.hadoop.fs.Path;

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
import com.inmobi.grill.server.api.query.QueryOutputFormatter;
import com.inmobi.grill.server.api.query.QueryResultFormatFailed;
import com.inmobi.grill.server.api.query.QueryResultFormatted;
import com.inmobi.grill.server.api.query.QuerySuccess;

public class ResultFormatter extends AsyncEventListener<QuerySuccess> {

  public static final Log LOG = LogFactory.getLog(ResultFormatter.class);
  QueryExecutionServiceImpl queryService;
  public ResultFormatter(QueryExecutionServiceImpl queryService) {
    this.queryService = queryService;
  }

  @Override
  public void process(QuerySuccess event) {
    formatOutput(event);    
  }

  private void formatOutput(QuerySuccess event) {
    QueryHandle queryHandle = event.getQueryHandle();
    LOG.info("Result formatter for " + queryHandle);

    QueryContext ctx;
    try {
      ctx = queryService.getQueryContext(queryHandle);
      if (ctx.isResultFormatted()) {
        LOG.info("Result already formatted. Skipping " + queryHandle);
        return;
      }
      if (!ctx.isPersistent()) {
        LOG.info("No result formatting required for query " + queryHandle);
        return;
      }
      LOG.info("Result formatting required. persistent:" + ctx.isPersistent());
      if (ctx.isResultAvailableInDriver()) {
        LOG.info("Result formatter for " + queryHandle);
        GrillResultSet resultSet = queryService.getDriverResultset(queryHandle);
        QueryOutputFormatter formatter = ctx.getQueryOutputFormatter();
        formatter.init(ctx, resultSet.getMetadata());
        if (ctx.getConf().getBoolean(GrillConfConstants.QUERY_OUTPUT_WRITE_HEADER,
            GrillConfConstants.DEFAULT_OUTPUT_WRITE_HEADER)) {
          formatter.writeHeader();
        }
        if (resultSet instanceof PersistentResultSet) {
          LOG.info("Result formatter for " + queryHandle + " in persistent result");
          //write all files from persistent directory
          ((PersistedOutputFormatter)formatter).addRowsFromPersistedPath(new Path(ctx.getHdfsoutPath()));
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
        formatter.close();
        ctx.setResultFormatted(true);
        queryService.getEventService().notifyEvent(new QueryResultFormatted(
            System.currentTimeMillis(), null, formatter.getFinalOutputPath().toString(), queryHandle));
        LOG.info("Result formatter has completed");
      }
    } catch (Exception e) {
      LOG.warn("Exception while formatting result for " + queryHandle, e);
      try {
        queryService.getEventService().notifyEvent(new QueryResultFormatFailed(
            System.currentTimeMillis(), null, e.getLocalizedMessage(), queryHandle));
      } catch (GrillException e1) {
        LOG.warn("Exception while sending formatting failure notification for " + queryHandle, e1);
      }
    }
  }
}
