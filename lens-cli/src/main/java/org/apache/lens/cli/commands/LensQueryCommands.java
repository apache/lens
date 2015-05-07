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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.UUID;

import org.apache.lens.api.query.*;
import org.apache.lens.cli.commands.annotations.UserDocumentation;
import org.apache.lens.client.LensClient;

import org.apache.commons.lang.StringUtils;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import com.google.common.base.Joiner;

/**
 * The Class LensQueryCommands.
 */
@Component
@UserDocumentation(title = "Query Management",
  description = "This section provides commands for query life cycle - "
    + "submit, check status,\n"
    + "  fetch results, kill or list all the queries. Also provides commands for\n"
    + "  prepare a query, destroy a prepared query and list all prepared queries.\n"
    + "\n"
    + "  Please note that, character <<<\">>> is used as delimiter by the Spring Shell\n"
    + "  framework, which is used to build lens cli. So queries which require <<<\">>>,\n"
    + "  should be prefixed with another double quote. For example\n"
    + "  <<<query execute cube select id,name from dim_table where name != \"\"first\"\">>>,\n"
    + "  will be parsed as <<<cube select id,name from dim_table where name != \"first\">>>")
public class LensQueryCommands extends BaseLensCommand {

  /**
   * Execute query.
   *
   * @param sql       the sql
   * @param async    the asynch
   * @param queryName the query name
   * @return the string
   */
  @CliCommand(value = "query execute",
    help = "Execute query <query-string>."
      +
      " If <async> is true, The query is launched in async manner and query handle is returned. It's by default false."
      + " <query name> can also be provided, though not required")
  public String executeQuery(
    @CliOption(key = {"", "query"}, mandatory = true, help = "<query-string>") String sql,
    @CliOption(key = {"async"}, mandatory = false, unspecifiedDefaultValue = "false",
      specifiedDefaultValue = "true", help = "<async>") boolean async,
    @CliOption(key = {"name"}, mandatory = false, help = "<query-name>") String queryName) {
    if (async) {
      QueryHandle handle = getClient().executeQueryAsynch(sql, queryName);
      return handle.getHandleId().toString();
    } else {
      try {
        LensClient.LensClientResultSetWithStats result = getClient().getResults(sql, queryName);
        return formatResultSet(result);
      } catch (Throwable t) {
        return t.getMessage();
      }
    }
  }

  /**
   * Format result set.
   *
   * @param rs the rs
   * @return the string
   */
  private String formatResultSet(LensClient.LensClientResultSetWithStats rs) {
    StringBuilder b = new StringBuilder();
    int numRows = 0;
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
          numRows++;
          b.append("\n");
        }
      } else {
        PersistentQueryResult temp = (PersistentQueryResult) r;
        numRows = temp.getNumRows();
        b.append("Results of query stored at : ").append(temp.getPersistedURI()).append(" ");
      }
    }

    if (rs.getQuery() != null) {
      long submissionTime = rs.getQuery().getSubmissionTime();
      long endTime = rs.getQuery().getFinishTime();
      b.append(numRows).append(" rows process in (").append(endTime > 0 ? ((endTime - submissionTime) / 1000) : 0)
        .append(") seconds.\n");
    }
    return b.toString();
  }

  /**
   * Gets the status.
   *
   * @param qh the qh
   * @return the status
   */
  @CliCommand(value = "query status", help = "Fetch status of executed query having query handle <query-handle>")
  public String getStatus(
    @CliOption(key = {"", "query-handle"}, mandatory = true, help = "<query-handle>") String qh) {
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
   * Gets the query details.
   *
   * @param qh the qh
   * @return the query
   */
  @CliCommand(value = "query details", help = "Get query details of query with handle <query-handle>")
  public String getDetails(
    @CliOption(key = {"", "query-handle"}, mandatory = true, help
      = "<query-handle>") String qh) {
    LensQuery query = getClient().getQueryDetails(qh);
    if (query == null) {
      return "Unable to find query for " + qh;
    }

    try {
      return formatJson(mapper.writer(pp).writeValueAsString(query));
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Explain query.
   *
   * @param sql      the sql
   * @param location the location
   * @return the string
   * @throws UnsupportedEncodingException the unsupported encoding exception
   */
  @CliCommand(value = "query explain",
    help = "Explain execution plan of query <query-string>. Can optionally save the plan"
      + " to a file by providing <save-location>")
  public String explainQuery(@CliOption(key = {"", "query"}, mandatory = true, help = "<query-string>") String sql,
    @CliOption(key = {"save-location"}, mandatory = false, help = "<save-location>") String location)
    throws IOException {
    QueryPlan plan = getClient().getQueryPlan(sql);
    if (plan.isError()) {
      return "Explain FAILED:" + plan.getErrorMsg();
    }
    if (StringUtils.isNotBlank(location)) {
      String validPath = getValidPath(location);
      OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(validPath), Charset.defaultCharset());
      osw.write(plan.getPlanString());
      osw.close();
      return "Saved to " + validPath;
    }
    return plan.getPlanString();
  }

  /**
   * Gets the all queries.
   *
   * @param state     the state
   * @param queryName the query name
   * @param user      the user
   * @param fromDate  the from date
   * @param toDate    the to date
   * @return the all queries
   */
  @CliCommand(value = "query list",
    help = "Get all queries. Various filter options can be provided(optionally), "
      + " as can be seen from the command syntax")
  public String getAllQueries(
    @CliOption(key = {"state"}, mandatory = false, help = "<query-status>") String state,
    @CliOption(key = {"name"}, mandatory = false, help = "<query-name>") String queryName,
    @CliOption(key = {"user"}, mandatory = false, help = "<user-who-submitted-query>") String user,
    @CliOption(key = {"fromDate"}, mandatory = false, unspecifiedDefaultValue = "-1", help
      = "<submission-time-is-after>") long fromDate,
    @CliOption(key = {"toDate"}, mandatory = false, unspecifiedDefaultValue = "" + Long.MAX_VALUE, help
      = "<submission-time-is-before>") long toDate) {
    List<QueryHandle> handles = getClient().getQueries(state, queryName, user, fromDate, toDate);
    if (handles != null && !handles.isEmpty()) {
      return Joiner.on("\n").skipNulls().join(handles).concat("\n").concat("Total number of queries: "
        + handles.size());
    } else {
      return "No queries";
    }
  }

  /**
   * Kill query.
   *
   * @param qh the qh
   * @return the string
   */
  @CliCommand(value = "query kill", help = "Kill query with handle <query-handle>")
  public String killQuery(
    @CliOption(key = {"", "query-handle"}, mandatory = true, help = "<query-handle>") String qh) {
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
   * @param qh the qh
   * @return the query results
   */
  @CliCommand(value = "query results", help = "get results of async query with query handle <query-handle>")
  public String getQueryResults(
    @CliOption(key = {"", "query-handle"}, mandatory = true, help = "<query-handle>") String qh) {
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
   * @param userName  the user name
   * @param queryName the query name
   * @param fromDate  the from date
   * @param toDate    the to date
   * @return the all prepared queries
   */
  @CliCommand(value = "prepQuery list",
    help = "Get all prepared queries. Various filters can be provided(optionally)"
      + " as can be seen from command syntax")
  public String getAllPreparedQueries(
    @CliOption(key = {"name"}, mandatory = false, help = "<query-name>") String queryName,
    @CliOption(key = {"user"}, mandatory = false, help = "<user-who-submitted-query>") String userName,
    @CliOption(key = {"fromDate"}, mandatory = false, unspecifiedDefaultValue = "-1", help
      = "<submission-time-is-after>") long fromDate,
    @CliOption(key = {"toDate"}, mandatory = false, unspecifiedDefaultValue = "" + Long.MAX_VALUE, help
      = "<submission-time-is-before>") long toDate) {
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
   * @param ph the ph
   * @return the prepared status
   */
  @CliCommand(value = "prepQuery details", help = "Get prepared query with handle <prepare-handle>")
  public String getPreparedStatus(
    @CliOption(key = {"", "prepare-handle"}, mandatory = true, help = "<prepare-handle>") String ph) {
    LensPreparedQuery prepared = getClient().getPreparedQuery(QueryPrepareHandle.fromString(ph));
    if (prepared != null) {
      StringBuilder sb = new StringBuilder()
        .append("User query:").append(prepared.getUserQuery()).append("\n")
        .append("Prepare handle:").append(prepared.getPrepareHandle()).append("\n")
        .append("User:" + prepared.getPreparedUser()).append("\n")
        .append("Prepared at:").append(prepared.getPreparedTime()).append("\n")
        .append("Selected driver :").append(prepared.getSelectedDriverClassName()).append("\n")
        .append("Driver query:").append(prepared.getDriverQuery()).append("\n");
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
   * @param ph the ph
   * @return the string
   */
  @CliCommand(value = "prepQuery destroy", help = "Destroy prepared query with handle <prepare-handle>")
  public String destroyPreparedQuery(
    @CliOption(key = {"", "prepare-handle"}, mandatory = true, help = "<prepare-handle>") String ph) {
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
   * @param phandle   the phandle
   * @param async    the asynch
   * @param queryName the query name
   * @return the string
   */
  @CliCommand(value = "prepQuery execute",
    help = "Execute prepared query with handle <prepare-handle>."
      + " If <async> is supplied and is true, query is run in async manner and query handle is returned immediately."
      + " Optionally, <query-name> can be provided, though not required.")
  public String executePreparedQuery(
    @CliOption(key = {"", "prepare-handle"}, mandatory = true, help = "Prepare handle to execute") String phandle,
    @CliOption(key = {"async"}, mandatory = false, unspecifiedDefaultValue = "false",
      specifiedDefaultValue = "true", help = "<async>") boolean async,
    @CliOption(key = {"name"}, mandatory = false, help = "<query-name>") String queryName) {
    if (async) {
      QueryHandle handle = getClient().executePrepared(QueryPrepareHandle.fromString(phandle), queryName);
      return handle.getHandleId().toString();
    } else {
      try {
        LensClient.LensClientResultSetWithStats result = getClient().getResultsFromPrepared(
          QueryPrepareHandle.fromString(phandle), queryName);
        return formatResultSet(result);
      } catch (Throwable t) {
        return t.getMessage();
      }
    }
  }

  /**
   * Prepare.
   *
   * @param sql       the sql
   * @param queryName the query name
   * @return the string
   * @throws UnsupportedEncodingException the unsupported encoding exception
   */
  @CliCommand(value = "prepQuery prepare",
    help = "Prepapre query <query-string> and return prepare handle. Can optionaly provide <query-name>")
  public String prepare(@CliOption(key = {"", "query"}, mandatory = true, help = "<query-string>") String sql,
    @CliOption(key = {"name"}, mandatory = false, help = "<query-name>") String queryName)
    throws UnsupportedEncodingException {
    return getClient().prepare(sql, queryName).toString();
  }

  /**
   * Explain and prepare.
   *
   * @param sql       the sql
   * @param queryName the query name
   * @return the string
   * @throws UnsupportedEncodingException the unsupported encoding exception
   */
  @CliCommand(value = "prepQuery explain",
    help = "Explain and prepare query <query-string>. Can optionally provide <query-name>")
  public String explainAndPrepare(
    @CliOption(key = {"", "query"}, mandatory = true, help = "<query-string>") String sql,
    @CliOption(key = {"name"}, mandatory = false, help = "<query-name>") String queryName)
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
