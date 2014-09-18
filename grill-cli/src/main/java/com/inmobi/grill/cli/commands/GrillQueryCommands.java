package com.inmobi.grill.cli.commands;

/*
 * #%L
 * Grill CLI
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

import com.google.common.base.Joiner;
import com.inmobi.grill.api.query.*;
import com.inmobi.grill.client.GrillClient;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.UUID;

@Component
public class GrillQueryCommands extends  BaseGrillCommand implements CommandMarker {

  @CliCommand(value = "query execute", help = "Execute query in async/sync manner")
  public String executeQuery(
      @CliOption(key = {"", "query"}, mandatory = true, help = "Query to execute") String sql,
      @CliOption(key = {"async"}, mandatory = false, unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true", help = "Sync query execution") boolean asynch) {
    if (!asynch) {
      try {
        GrillClient.GrillClientResultSetWithStats result = getClient().getResults(sql);
        return formatResultSet(result);
      } catch (Throwable t) {
        return t.getMessage();
      }
    } else {
      QueryHandle handle = getClient().executeQueryAsynch(sql);
      return handle.getHandleId().toString();
    }
  }

  private String formatResultSet(GrillClient.GrillClientResultSetWithStats rs) {
    StringBuilder b = new StringBuilder();
    int i = 0;
    if (rs.getResultSet() != null) {
      QueryResultSetMetadata resultSetMetadata = rs.getResultSet().getResultSetMetadata();
      for (ResultColumn column : resultSetMetadata.getColumns()) {
        b.append(column.getName()).append("\t");
      }
      b.append("\n");
      QueryResult r = rs.getResultSet().getResult();
      if (r instanceof InMemoryQueryResult) {
        InMemoryQueryResult temp = (InMemoryQueryResult) r;
        for (ResultRow row : temp.getRows()) {
          for (Object col : row.getValues()) {
            b.append(col).append("\t");
          }
          i++;
          b.append("\n");
        }
      } else {
        PersistentQueryResult temp = (PersistentQueryResult) r;
        b.append("Results of query stored at : " + temp.getPersistedURI());
      }
    }

    if (rs.getQuery() != null) {
      long submissionTime = rs.getQuery().getSubmissionTime();
      long endTime = rs.getQuery().getFinishTime();
      b.append(i).append(" rows process in (").
          append(endTime>0?((endTime - submissionTime)/1000): 0).
          append(") seconds.\n");
    }
    return b.toString();
  }

  @CliCommand(value = "query status", help = "Fetch status of executed query")
  public String getStatus(@CliOption(key = {"", "query"},
      mandatory = true, help = "<query-handle> for which status has to be fetched") String qh) {
    QueryStatus status = getClient().getQueryStatus(new QueryHandle(UUID.fromString(qh)));
    StringBuilder sb = new StringBuilder();
    if(status == null) {
      return "Unable to find status for " + qh;
    }
    sb.append("Status : ").append(status.getStatus()).append("\n");
    if (status.getStatusMessage() != null) {
      sb.append("Message : ").append(status.getStatusMessage()).append("\n");
    }
    if (status.getProgress() != 0) {
      sb.append("Progress : ").append(status.getProgress()).append("\n");
      if (status.getProgressMessage() != null) {
        sb.append("Progress Message : ").append(status.getProgressMessage()).append("\n");
      }
    }

    if (status.getErrorMessage() != null) {
      sb.append("Error : ").append(status.getErrorMessage()).append("\n");
    }

    return sb.toString();
  }

  @CliCommand(value = "query explain", help = "Explain query plan")
  public String explainQuery(@CliOption(key = {"", "query"}, mandatory = true,
      help = "Query to execute") String sql, @CliOption(key = {"save"},
      mandatory = false, help = "query to explain") String location)
      throws UnsupportedEncodingException {
    
    QueryPlan plan = getClient().getQueryPlan(sql);
    if (plan.isHasError() == true) {
      return plan.getErrorMsg();
    }
    return plan.getPlanString();
  }

  @CliCommand(value = "query list", help = "Get all queries")
  public String getAllQueries(@CliOption(key = {"state"}, mandatory = false,
      help = "Status of queries to be listed") String state, @CliOption(key = {"user"}, mandatory = false,
      help = "User of queries to be listed") String user) {
    List<QueryHandle> handles = getClient().getQueries(state, user);
    if (handles != null && !handles.isEmpty()) {
      return Joiner.on("\n").skipNulls().join(handles);
    } else {
      return "No queries";
    }
  }

  @CliCommand(value = "query kill", help ="Kill a query")
  public String killQuery(@CliOption(key = {"", "query"},
      mandatory = true, help = "query-handle for killing") String qh) {
    boolean status = getClient().killQuery(new QueryHandle(UUID.fromString(qh)));
    if(status) {
      return "Successfully killed " + qh;
    } else {
      return "Failed in killing "  + qh;
    }
  }

  @CliCommand(value = "query results", help ="get results of async query")
  public String getQueryResults(
      @CliOption(key = {"", "query"}, mandatory = true, help = "query-handle for fetching result") String qh)   {
    try {
      GrillClient.GrillClientResultSetWithStats result = getClient().getAsyncResults(
          new QueryHandle(UUID.fromString(qh)));
      return formatResultSet(result);
    } catch (Throwable t) {
      return t.getMessage();
    }
  }

  @CliCommand(value = "prepQuery list", help = "Get all prepared queries")
  public String getAllPreparedQueries() {
    List<QueryPrepareHandle> handles = getClient().getPreparedQueries();
    if (handles != null && !handles.isEmpty()) {
      return Joiner.on("\n").skipNulls().join(handles);
    } else {
      return "No prepared queries";
    }
  }

  @CliCommand(value = "prepQuery details", help = "Get prepared query")
  public String getPreparedStatus(@CliOption(key = {"", "handle"},
      mandatory = true, help = "Prepare handle") String ph) {
    GrillPreparedQuery prepared = getClient().getPreparedQuery(QueryPrepareHandle.fromString(ph));
    if (prepared != null) {
      StringBuilder sb = new StringBuilder();
      sb.append("User query:").append(prepared.getUserQuery()).append("\n");
      sb.append("Prepare handle:").append(prepared.getPrepareHandle()).append("\n");
      sb.append("User:" + prepared.getPreparedUser()).append("\n");
      sb.append("Prepared at:").append(prepared.getPreparedTime()).append("\n");
      sb.append("Selected driver :").append(prepared.getSelectedDriverClassName()).append("\n");
      sb.append("Driver query:").append(prepared.getDriverQuery()).append("\n");
      if (prepared.getConf() != null) {
        sb.append("Conf:").append(prepared.getConf().getProperties()).append("\n");
      }

      return sb.toString();
    } else {
      return "No such handle";
    }
  }

  @CliCommand(value = "prepQuery destroy", help ="Destroy a prepared query")
  public String destroyPreparedQuery(@CliOption(key = {"", "handle"},
      mandatory = true, help = "prepare handle to destroy") String ph) {
    boolean status = getClient().destroyPrepared(new QueryPrepareHandle(UUID.fromString(ph)));
    if(status) {
      return "Successfully destroyed " + ph;
    } else {
      return "Failed in destroying "  + ph;
    }
  }

  @CliCommand(value = "prepQuery execute", help = "Execute prepared query in async/sync manner")
  public String executePreparedQuery(
      @CliOption(key = {"", "handle"}, mandatory = true, help = "Prepare handle to execute") String phandle,
      @CliOption(key = {"async"}, mandatory = false, unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true", help = "Sync query execution") boolean asynch) {
    if (!asynch) {
      try {
        GrillClient.GrillClientResultSetWithStats result =
            getClient().getResultsFromPrepared(QueryPrepareHandle.fromString(phandle));
        return formatResultSet(result);
      } catch (Throwable t) {
        return t.getMessage();
      }
    } else {
      QueryHandle handle = getClient().executePrepared(QueryPrepareHandle.fromString(phandle));
      return handle.getHandleId().toString();
    }
  }

  @CliCommand(value = "prepQuery prepare", help = "Prepapre query")
  public String prepare(@CliOption(key = {"", "query"}, mandatory = true,
      help = "Query to prepare") String sql)
      throws UnsupportedEncodingException {

    QueryPrepareHandle handle = getClient().prepare(sql);
    return handle.toString();
  }

  @CliCommand(value = "prepQuery explain", help = "Explain and prepare query")
  public String explainAndPrepare(@CliOption(key = {"", "query"}, mandatory = true,
      help = "Query to explain and prepare") String sql)
      throws UnsupportedEncodingException {

    QueryPlan plan = getClient().explainAndPrepare(sql);
    StringBuilder planStr = new StringBuilder(plan.getPlanString());
    planStr.append("\n").append("Prepare handle:").append(plan.getPrepareHandle());
    return planStr.toString();
  }

}
