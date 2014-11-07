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
package org.apache.lens.cli.commands;

import com.google.common.base.Joiner;

import org.apache.lens.api.query.*;
import org.apache.lens.client.LensClient;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.UUID;

/**
 * The Class LensQueryCommands.
 */
@Component
public class LensQueryCommands extends BaseLensCommand implements CommandMarker {

  /**
   * Execute query.
   *
   * @param sql
   *          the sql
   * @param asynch
   *          the asynch
   * @param queryName
   *          the query name
   * @return the string
   */
  @CliCommand(value = "query execute", help = "Execute query in async/sync manner")
  public String executeQuery(
      @CliOption(key = { "", "query" }, mandatory = true, help = "Query to execute") String sql,
      @CliOption(key = { "async" }, mandatory = false, unspecifiedDefaultValue = "false", specifiedDefaultValue = "true", help = "Sync query execution") boolean asynch,
      @CliOption(key = { "name" }, mandatory = false, help = "Query name") String queryName) {
    if (!asynch) {
      try {
        LensClient.LensClientResultSetWithStats result = getClient().getResults(sql, queryName);
        return formatResultSet(result);
      } catch (Throwable t) {
        return t.getMessage();
      }
    } else {
      QueryHandle handle = getClient().executeQueryAsynch(sql, queryName);
      return handle.getHandleId().toString();
    }
  }

  /**
   * Format result set.
   *
   * @param rs
   *          the rs
   * @return the string
   */
  private String formatResultSet(LensClient.LensClientResultSetWithStats rs) {
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
        b.append("Results of query stored at : ").append(temp.getPersistedURI()).append(" ");
      }
    }

    if (rs.getQuery() != null) {
      long submissionTime = rs.getQuery().getSubmissionTime();
      long endTime = rs.getQuery().getFinishTime();
      b.append(i).append(" rows process in (").append(endTime > 0 ? ((endTime - submissionTime) / 1000) : 0)
      .append(") seconds.\n");
    }
    return b.toString();
  }

  /**
   * Gets the status.
   *
   * @param qh
   *          the qh
   * @return the status
   */
  @CliCommand(value = "query status", help = "Fetch status of executed query")
  public String getStatus(
      @CliOption(key = { "", "query" }, mandatory = true, help = "<query-handle> for which status has to be fetched") String qh) {
    QueryStatus status = getClient().getQueryStatus(new QueryHandle(UUID.fromString(qh)));
    StringBuilder sb = new StringBuilder();
    if (status == null) {
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

  /**
   * Explain query.
   *
   * @param sql
   *          the sql
   * @param location
   *          the location
   * @return the string
   * @throws UnsupportedEncodingException
   *           the unsupported encoding exception
   */
  @CliCommand(value = "query explain", help = "Explain query plan")
  public String explainQuery(@CliOption(key = { "", "query" }, mandatory = true, help = "Query to execute") String sql,
      @CliOption(key = { "save" }, mandatory = false, help = "query to explain") String location)
          throws UnsupportedEncodingException {

    QueryPlan plan = getClient().getQueryPlan(sql);
    if (plan.isError()) {
      return "Explain FAILED:" + plan.getErrorMsg();
    }
    return plan.getPlanString();
  }

  /**
   * Gets the all queries.
   *
   * @param state
   *          the state
   * @param queryName
   *          the query name
   * @param user
   *          the user
   * @param fromDate
   *          the from date
   * @param toDate
   *          the to date
   * @return the all queries
   */
  @CliCommand(value = "query list", help = "Get all queries")
  public String getAllQueries(
      @CliOption(key = { "state" }, mandatory = false, help = "Status of queries to be listed") String state,
      @CliOption(key = { "name" }, mandatory = false, help = "query name") String queryName,
      @CliOption(key = { "user" }, mandatory = false, help = "user name. Use 'all' to get queries of all users") String user,
      @CliOption(key = { "fromDate" }, mandatory = false, unspecifiedDefaultValue = "-1", help = "start time to filter queries by submission time") long fromDate,
      @CliOption(key = { "toDate" }, mandatory = false, unspecifiedDefaultValue = "-1", help = "end time to filter queries by submission time") long toDate) {

    if (toDate == -1L) {
      toDate = Long.MAX_VALUE;
    }
    List<QueryHandle> handles = getClient().getQueries(state, queryName, user, fromDate, toDate);
    if (handles != null && !handles.isEmpty()) {
      return Joiner.on("\n").skipNulls().join(handles);
    } else {
      return "No queries";
    }
  }

  /**
   * Kill query.
   *
   * @param qh
   *          the qh
   * @return the string
   */
  @CliCommand(value = "query kill", help = "Kill a query")
  public String killQuery(
      @CliOption(key = { "", "query" }, mandatory = true, help = "query-handle for killing") String qh) {
    boolean status = getClient().killQuery(new QueryHandle(UUID.fromString(qh)));
    if (status) {
      return "Successfully killed " + qh;
    } else {
      return "Failed in killing " + qh;
    }
  }

  /**
   * Gets the query results.
   *
   * @param qh
   *          the qh
   * @return the query results
   */
  @CliCommand(value = "query results", help = "get results of async query")
  public String getQueryResults(
      @CliOption(key = { "", "query" }, mandatory = true, help = "query-handle for fetching result") String qh) {
    try {
      LensClient.LensClientResultSetWithStats result = getClient()
          .getAsyncResults(new QueryHandle(UUID.fromString(qh)));
      return formatResultSet(result);
    } catch (Throwable t) {
      return t.getMessage();
    }
  }

  /**
   * Gets the all prepared queries.
   *
   * @param userName
   *          the user name
   * @param queryName
   *          the query name
   * @param fromDate
   *          the from date
   * @param toDate
   *          the to date
   * @return the all prepared queries
   */
  @CliCommand(value = "prepQuery list", help = "Get all prepared queries")
  public String getAllPreparedQueries(
      @CliOption(key = { "user" }, mandatory = false, help = "user name") String userName,
      @CliOption(key = { "name" }, mandatory = false, help = "query name") String queryName,
      @CliOption(key = { "fromDate" }, mandatory = false, unspecifiedDefaultValue = "-1", help = "start time to filter queries by submission time") long fromDate,
      @CliOption(key = { "toDate" }, mandatory = false, unspecifiedDefaultValue = "-1", help = "end time to filter queries by submission time") long toDate) {

    if (toDate == -1L) {
      toDate = Long.MAX_VALUE;
    }
    List<QueryPrepareHandle> handles = getClient().getPreparedQueries(userName, queryName, fromDate, toDate);
    if (handles != null && !handles.isEmpty()) {
      return Joiner.on("\n").skipNulls().join(handles);
    } else {
      return "No prepared queries";
    }
  }

  /**
   * Gets the prepared status.
   *
   * @param ph
   *          the ph
   * @return the prepared status
   */
  @CliCommand(value = "prepQuery details", help = "Get prepared query")
  public String getPreparedStatus(
      @CliOption(key = { "", "handle" }, mandatory = true, help = "Prepare handle") String ph) {
    LensPreparedQuery prepared = getClient().getPreparedQuery(QueryPrepareHandle.fromString(ph));
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

  /**
   * Destroy prepared query.
   *
   * @param ph
   *          the ph
   * @return the string
   */
  @CliCommand(value = "prepQuery destroy", help = "Destroy a prepared query")
  public String destroyPreparedQuery(
      @CliOption(key = { "", "handle" }, mandatory = true, help = "prepare handle to destroy") String ph) {
    boolean status = getClient().destroyPrepared(new QueryPrepareHandle(UUID.fromString(ph)));
    if (status) {
      return "Successfully destroyed " + ph;
    } else {
      return "Failed in destroying " + ph;
    }
  }

  /**
   * Execute prepared query.
   *
   * @param phandle
   *          the phandle
   * @param asynch
   *          the asynch
   * @param queryName
   *          the query name
   * @return the string
   */
  @CliCommand(value = "prepQuery execute", help = "Execute prepared query in async/sync manner")
  public String executePreparedQuery(
      @CliOption(key = { "", "handle" }, mandatory = true, help = "Prepare handle to execute") String phandle,
      @CliOption(key = { "async" }, mandatory = false, unspecifiedDefaultValue = "false", specifiedDefaultValue = "true", help = "Sync query execution") boolean asynch,
      @CliOption(key = { "name" }, mandatory = false, help = "query name") String queryName) {
    if (!asynch) {
      try {
        LensClient.LensClientResultSetWithStats result = getClient().getResultsFromPrepared(
            QueryPrepareHandle.fromString(phandle), queryName);
        return formatResultSet(result);
      } catch (Throwable t) {
        return t.getMessage();
      }
    } else {
      QueryHandle handle = getClient().executePrepared(QueryPrepareHandle.fromString(phandle), queryName);
      return handle.getHandleId().toString();
    }
  }

  /**
   * Prepare.
   *
   * @param sql
   *          the sql
   * @param queryName
   *          the query name
   * @return the string
   * @throws UnsupportedEncodingException
   *           the unsupported encoding exception
   */
  @CliCommand(value = "prepQuery prepare", help = "Prepapre query")
  public String prepare(@CliOption(key = { "", "query" }, mandatory = true, help = "Query to prepare") String sql,
      @CliOption(key = { "name" }, mandatory = false, help = "query name") String queryName)
          throws UnsupportedEncodingException {

    QueryPrepareHandle handle = getClient().prepare(sql, queryName);
    return handle.toString();
  }

  /**
   * Explain and prepare.
   *
   * @param sql
   *          the sql
   * @param queryName
   *          the query name
   * @return the string
   * @throws UnsupportedEncodingException
   *           the unsupported encoding exception
   */
  @CliCommand(value = "prepQuery explain", help = "Explain and prepare query")
  public String explainAndPrepare(
      @CliOption(key = { "", "query" }, mandatory = true, help = "Query to explain and prepare") String sql,
      @CliOption(key = { "name" }, mandatory = false, help = "query name") String queryName)
          throws UnsupportedEncodingException {

    QueryPlan plan = getClient().explainAndPrepare(sql, queryName);
    if (plan.isError()) {
      return "Explain FAILED:" + plan.getErrorMsg();
    }
    StringBuilder planStr = new StringBuilder(plan.getPlanString());
    planStr.append("\n").append("Prepare handle:").append(plan.getPrepareHandle());
    return planStr.toString();
  }

}
