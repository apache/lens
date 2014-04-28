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
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.query.*;
import com.inmobi.grill.client.GrillClient;
import com.inmobi.grill.client.GrillClientResultSet;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.UUID;

@Component
public class GrillQueryCommands implements CommandMarker {
  private GrillClient client;


  public void setClient(GrillClient client) {
    this.client = client;
  }

  @CliCommand(value = "query execute", help = "Execute query in async/sync manner")
  public String executeQuery(
      @CliOption(key = {"", "query"}, mandatory = true, help = "Query to execute") String sql,
      @CliOption(key = {"async"}, mandatory = false, unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true", help = "Sync query execution") boolean asynch) {
    if (!asynch) {
      try {
        GrillClientResultSet result = client.getResults(sql);
        return formatResultSet(result);
      } catch (Throwable t) {
        return t.getMessage();
      }
    } else {
      QueryHandle handle = client.executeQueryAsynch(sql);
      return handle.getHandleId().toString();
    }
  }

  private String formatResultSet(GrillClientResultSet result) {
    StringBuilder b = new StringBuilder();
    QueryResultSetMetadata resultSetMetadata = result.getResultSetMetadata();
    for (ResultColumn column : resultSetMetadata.getColumns()) {
      b.append(column.getName()).append("\t");
    }
    b.append("\n");
    QueryResult r = result.getResult();
    if (r instanceof InMemoryQueryResult) {
      InMemoryQueryResult temp = (InMemoryQueryResult) r;
      for (ResultRow row : temp.getRows()) {
        for (Object col : row.getValues()) {
          b.append(col).append("\t");
        }
        b.append("\n");
      }
    } else {
      PersistentQueryResult temp = (PersistentQueryResult) r;
      b.append("Results of query stored at : " + temp.getPersistedURI());
    }
    return b.toString();
  }

  @CliCommand(value = "query status", help = "Fetch status of executed query")
  public String getStatus(@CliOption(key = {"", "query"},
      mandatory = true, help = "Query to execute") String qh) {
    QueryStatus status = client.getQueryStatus(new QueryHandle(UUID.fromString(qh)));
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
  public String getQueries(@CliOption(key = {"", "query"}, mandatory = true,
      help = "Query to execute") String sql, @CliOption(key = {"save"},
      mandatory = false, help = "Sync query execution") String location)
          throws UnsupportedEncodingException {

    QueryPlan plan = client.getQueryPlan(sql);
    return plan.getPlanString();
  }

  @CliCommand(value = "query list", help = "Get all Queries in the session")
  public String getAllQueries() {
    List<QueryHandle> handles = client.getQueries();
    if(handles != null) {
      return Joiner.on("\n").skipNulls().join(handles);
    } else {
      return "No Queries are associated with session";
    }
  }

  @CliCommand(value = "query kill", help ="Kill a query")
  public String killQuery(@CliOption(key = {"", "query"},
      mandatory = true, help = "Query to execute") String qh) {
    boolean status = client.killQuery(new QueryHandle(UUID.fromString(qh)));
    if(status) {
      return "Successfully killed " + qh;
    } else {
      return "Failed in killing "  + qh;
    }
  }

  @CliCommand(value = "query results", help ="get results of async query")
  public String getQueryResults(@CliOption(key = {"", "query"},
      mandatory = true, help = "Query to execute") String qh)   {
    try {
      GrillClientResultSet result = client.getAsyncResults(
          new QueryHandle(UUID.fromString(qh)));
      return formatResultSet(result);
    } catch (Throwable t) {
      return t.getMessage();
    }
  }
}
