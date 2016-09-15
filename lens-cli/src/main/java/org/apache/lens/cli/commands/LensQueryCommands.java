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

import java.io.*;
import java.nio.charset.Charset;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.query.*;
import org.apache.lens.api.result.PrettyPrintable;
import org.apache.lens.cli.commands.annotations.UserDocumentation;
import org.apache.lens.cli.config.LensCliConfigConstants;
import org.apache.lens.client.LensClient;
import org.apache.lens.client.LensClient.LensClientResultSetWithStats;
import org.apache.lens.client.LensClientResultSet;
import org.apache.lens.client.exceptions.LensAPIException;
import org.apache.lens.client.exceptions.LensBriefErrorException;
import org.apache.lens.client.model.BriefError;
import org.apache.lens.client.model.IdBriefErrorTemplate;
import org.apache.lens.client.model.IdBriefErrorTemplateKey;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import com.google.common.base.Joiner;

/**
 * The Class LensQueryCommands.
 * SUSPEND CHECKSTYLE CHECK InnerAssignmentCheck
 */
@Component
@UserDocumentation(title = "Commands for Query Management",
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
  public static final String DEFAULT_QUERY_HANDLE_DESCRIPTION =
    "If not provided, takes last query handle interacted with.";
  private static final String ASYNC_DOC =
    "If <async> is true, The query is launched in async manner and query handle is returned. It's by default false.";
  private static final String QUERY_NAME_DOC = "<query name> can also be provided, though not required.";

  @CliCommand(value = "select",
    help = "Execute query <select query-string-without-select>. " + ASYNC_DOC + " " + QUERY_NAME_DOC)
  public String executeSelectQuery(
    @CliOption(key = {""}, mandatory = true, help = "<query-string-without-select>") String sql,
    @CliOption(key = {"async"}, mandatory = false, unspecifiedDefaultValue = "false",
      specifiedDefaultValue = "true", help = "<async>") boolean async,
    @CliOption(key = {"name"}, mandatory = false, help = "<query-name>") String queryName) {
    return executeQuery("select " + sql, async, queryName);
  }

  @CliCommand(value = "cube select",
    help = "Execute cube query <cube select query-string-without-cube-select>. " + ASYNC_DOC + " " + QUERY_NAME_DOC)
  public String executeCubeSelectQuery(
    @CliOption(key = {""}, mandatory = true, help = "<query-string-without-cube-select>") String sql,
    @CliOption(key = {"async"}, mandatory = false, unspecifiedDefaultValue = "false",
      specifiedDefaultValue = "true", help = "<async>") boolean async,
    @CliOption(key = {"name"}, mandatory = false, help = "<query-name>") String queryName) {
    return executeQuery("cube select " + sql, async, queryName);
  }

    /**
     * Execute query.
     *
     * @param sql       the sql
     * @param async    the asynch
     * @param queryName the query name
     * @return the string
     */
  @CliCommand(value = "query execute",
    help = "Execute query <query-string>. " + ASYNC_DOC + " " + QUERY_NAME_DOC)
  public String executeQuery(
    @CliOption(key = {"", "query"}, mandatory = true, help = "<query-string>") String sql,
    @CliOption(key = {"async"}, mandatory = false, unspecifiedDefaultValue = "false",
      specifiedDefaultValue = "true", help = "<async>") boolean async,
    @CliOption(key = {"name"}, mandatory = false, help = "<query-name>") String queryName) {

    PrettyPrintable cliOutput;

    try {
      if (async) {
        QueryHandle queryHandle = getClient().executeQueryAsynch(sql, queryName, new LensConf());
        return queryHandle.getHandleIdString();
      } else {
        LensClientResultSetWithStats resultWithStats;
        long timeOutMillis = getClient().getConf().getLong(LensCliConfigConstants.QUERY_EXECUTE_TIMEOUT_MILLIS,
            LensCliConfigConstants.DEFAULT_QUERY_EXECUTE_TIMEOUT_MILLIS);
        LensClient.getCliLogger().info("Executing query with timeout of {} milliseconds", timeOutMillis);
        QueryHandleWithResultSet result = getClient().executeQueryWithTimeout(sql, queryName, timeOutMillis,
          new LensConf());
        if (result.getResult() == null) {
          //Query not finished yet. Wait till it finishes and get result.
          LensClient.getCliLogger().info("Couldn't complete query execution within timeout. Waiting for completion");
          resultWithStats = getClient().getSyncResults(result.getQueryHandle());
        } else {
          LensClientResultSet clientResultSet = new LensClientResultSet(result.getResultMetadata(), result.getResult());
          resultWithStats =
              new LensClientResultSetWithStats(clientResultSet, getClient().getQueryDetails(result.getQueryHandle()));
        }
        return formatResultSet(resultWithStats);
      }
    } catch (final LensAPIException e) {
      BriefError briefError = new BriefError(e.getLensAPIErrorCode(), e.getLensAPIErrorMessage());
      cliOutput = new IdBriefErrorTemplate(IdBriefErrorTemplateKey.REQUEST_ID, e.getLensAPIRequestId(), briefError);
    } catch (final LensBriefErrorException e) {
      cliOutput = e.getIdBriefErrorTemplate();
    }

    return cliOutput.toPrettyString();
  }

  /**
   * Format result set.
   *
   * @param rs the rs
   * @return the string
   */
  private String formatResultSet(LensClient.LensClientResultSetWithStats rs) {
    StringBuilder b = new StringBuilder();
    if (rs.getResultSet() != null) {
      QueryResultSetMetadata resultSetMetadata = rs.getResultSet().getResultSetMetadata();
      for (ResultColumn column : resultSetMetadata.getColumns()) {
        b.append(column.getName()).append("\t");
      }
      b.append("\n");
      QueryResult r = rs.getResultSet().getResult();
      if (r instanceof InMemoryQueryResult) {
        InMemoryQueryResult temp = (InMemoryQueryResult) r;
        b.append(temp.toPrettyString());
      } else {
        PersistentQueryResult temp = (PersistentQueryResult) r;
        b.append("Results of query stored at : ").append(temp.getPersistedURI()).append("  ");
        if (null != temp.getNumRows()) {
          b.append(temp.getNumRows()).append(" rows ");
        }
      }
    }

    if (rs.getQuery() != null) {
      long submissionTime = rs.getQuery().getSubmissionTime();
      long endTime = rs.getQuery().getFinishTime();
      b.append("processed in (").append(endTime > 0 ? ((endTime - submissionTime) / 1000) : 0)
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
  @CliCommand(value = "query status",
    help = "Fetch status of executed query having query handle <query_handle>. " + DEFAULT_QUERY_HANDLE_DESCRIPTION)
  public String getStatus(
    @CliOption(key = {"", "query_handle"}, mandatory = false, help = "<query_handle>") String qh) {
    qh = getOrDefaultQueryHandleString(qh);
    QueryHandle handle = QueryHandle.fromString(qh);
    QueryStatus status = getClient().getQueryStatus(handle);
    if (status == null) {
      return "Unable to find status for " + handle;
    }
    return "Query Handle: " + qh + "\n" + status.toString();
  }

  /**
   * Gets the query details.
   *
   * @param qh the qh
   * @return the query
   */
  @CliCommand(value = "query details",
    help = "Get query details of query with handle <query_handle>." + DEFAULT_QUERY_HANDLE_DESCRIPTION)
  public String getDetails(
    @CliOption(key = {"", "query_handle"}, mandatory = false, help = "<query_handle>") String qh) {
    qh = getOrDefaultQueryHandleString(qh);
    LensQuery query = getClient().getQueryDetails(qh);
    if (query == null) {
      return "Unable to find query for " + qh;
    }
    return formatJson(query);
  }

  /**
   * Explain query.
   *
   * @param sql      the sql
   * @return the string
   * @throws LensAPIException
   * @throws UnsupportedEncodingException the unsupported encoding exception
   */
  @CliCommand(value = "query explain",
    help = "Explain execution plan of query <query-string>. "
      + "Can optionally save the plan to a file by providing <save_location>")
  public String explainQuery(@CliOption(key = {"", "query"}, mandatory = true, help = "<query-string>") String sql,
    @CliOption(key = {"save_location"}, mandatory = false, help = "<save_location>") final File path)
    throws IOException, LensAPIException {
    PrettyPrintable cliOutput;

    try {
      QueryPlan plan = getClient().getQueryPlan(sql).getData();
      if (path != null && StringUtils.isNotBlank(path.getPath())) {
        String validPath = getValidPath(path, false, false);
        try (OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(validPath),
          Charset.defaultCharset())) {
          osw.write(plan.getPlanString());
        }
        return "Saved to " + validPath;
      }
      return plan.getPlanString();
    } catch (final LensAPIException e) {
      BriefError briefError = new BriefError(e.getLensAPIErrorCode(), e.getLensAPIErrorMessage());
      cliOutput = new IdBriefErrorTemplate(IdBriefErrorTemplateKey.REQUEST_ID, e.getLensAPIRequestId(), briefError);
    } catch (final LensBriefErrorException e) {
      cliOutput = e.getIdBriefErrorTemplate();
    }
    return cliOutput.toPrettyString();
  }

  /**
   * Gets the all queries.
   *
   * @param state     the state
   * @param queryName the query name
   * @param user      the user
   * @param driver    the driver name
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
    @CliOption(key = {"driver"}, mandatory = false, help = "<driver-where-query-ran>") String driver,
    @CliOption(key = {"fromDate"}, mandatory = false, help = "<submission-time-is-after>") String fromDate,
    @CliOption(key = {"toDate"}, mandatory = false, help = "<submission-time-is-before>") String toDate) {
    List<QueryHandle> handles = getClient().getQueries(state, queryName, user, driver, fromDate, toDate);
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
  @CliCommand(value = "query kill", help = "Kill query with handle <query_handle>." + DEFAULT_QUERY_HANDLE_DESCRIPTION)
  public String killQuery(@CliOption(key = {"", "query_handle"}, mandatory = false, help = "<query_handle>") String
    qh) {
    qh = getOrDefaultQueryHandleString(qh);
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
  @CliCommand(value = "query results",
    help = "get results of query with query handle <query_handle>. " + DEFAULT_QUERY_HANDLE_DESCRIPTION
      + "If async is false then wait till the query execution is completed, it's by default true. "
      + "Can optionally save the results to a file by providing <save_location>.")
  public String getQueryResults(
    @CliOption(key = {"", "query_handle"}, mandatory = false, help = "<query_handle>") String qh,
    @CliOption(key = {"save_location"}, mandatory = false, help = "<save_location>") final File path,
    @CliOption(key = {"async"}, mandatory = false, unspecifiedDefaultValue = "true",
      help = "<async>") boolean async) {
    qh = getOrDefaultQueryHandleString(qh);
    QueryHandle queryHandle = new QueryHandle(UUID.fromString(qh));
    LensClient.LensClientResultSetWithStats results;
    String location = path != null ? path.getPath() : null;
    try {
      if (StringUtils.isNotBlank(location)) {
        location = getValidPath(path, true, true);
        Response response = getClient().getHttpResults(queryHandle);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
          String disposition = (String) response.getHeaders().get("content-disposition").get(0);
          String fileName = disposition.split("=")[1].trim();
          location = getValidPath(new File(location + File.separator + fileName), false, false);
          try (InputStream stream = response.readEntity(InputStream.class);
               FileOutputStream outStream = new FileOutputStream(new File(location))) {
            IOUtils.copy(stream, outStream);
          }
          return "Saved to " + location;
        } else {
          if (async) {
            results = getClient().getAsyncResults(queryHandle);
          } else {
            results = getClient().getSyncResults(queryHandle);
          }
          if (results.getResultSet() == null) {
            return "Resultset not yet available";
          } else if (results.getResultSet().getResult() instanceof InMemoryQueryResult) {
            location = getValidPath(new File(location + File.separator + qh + ".csv"), false, false);
            try (OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(location),
              Charset.defaultCharset())) {
              osw.write(formatResultSet(results));
            }
            return "Saved to " + location;
          } else {
            return "Can't download the result because it's available in driver's persistence.\n"
              + formatResultSet(results);
          }
        }
      } else {
        if (async) {
          results = getClient().getAsyncResults(queryHandle);
        } else {
          results = getClient().getSyncResults(queryHandle);
        }
        return results.getResultSet() == null ? "Resultset not available for the query" : formatResultSet(results);
      }
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
    @CliOption(key = {"fromDate"}, mandatory = false, help = "<submission-time-is-after>") String fromDate,
    @CliOption(key = {"toDate"}, mandatory = false, help = "<submission-time-is-before>") String toDate) {
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
  @CliCommand(value = "prepQuery details", help = "Get prepared query with handle <prepare_handle>")
  public String getPreparedStatus(
    @CliOption(key = {"", "prepare_handle"}, mandatory = true, help = "<prepare_handle>") String ph) {
    LensPreparedQuery prepared = getClient().getPreparedQuery(QueryPrepareHandle.fromString(ph));
    if (prepared != null) {
      StringBuilder sb = new StringBuilder()
        .append("User query:").append(prepared.getUserQuery()).append("\n")
        .append("Prepare handle:").append(prepared.getPrepareHandle()).append("\n")
        .append("User:").append(prepared.getPreparedUser()).append("\n")
        .append("Prepared at:").append(prepared.getPreparedTime()).append("\n")
        .append("Selected driver :").append(prepared.getSelectedDriverName()).append("\n")
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
  @CliCommand(value = "prepQuery destroy", help = "Destroy prepared query with handle <prepare_handle>")
  public String destroyPreparedQuery(
    @CliOption(key = {"", "prepare_handle"}, mandatory = true, help = "<prepare_handle>") String ph) {
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
    help = "Execute prepared query with handle <prepare_handle>."
      + " If <async> is supplied and is true, query is run in async manner and query handle is returned immediately."
      + " Optionally, <query-name> can be provided, though not required.")
  public String executePreparedQuery(
    @CliOption(key = {"", "prepare_handle"}, mandatory = true, help = "Prepare handle to execute") String phandle,
    @CliOption(key = {"async"}, mandatory = false, unspecifiedDefaultValue = "false",
      specifiedDefaultValue = "true", help = "<async>") boolean async,
    @CliOption(key = {"name"}, mandatory = false, help = "<query-name>") String queryName) {
    try {
      if (async) {
        QueryHandle handle = getClient().executePrepared(QueryPrepareHandle.fromString(phandle), queryName);
        return handle.getHandleId().toString();
      } else {
        LensClient.LensClientResultSetWithStats result =
            getClient().getResultsFromPrepared(QueryPrepareHandle.fromString(phandle), queryName);
        return formatResultSet(result);
      }
    } catch (Throwable t) {
      return t.getMessage();
    }
  }

  /**
   * Prepare.
   *
   * @param sql       the sql
   * @param queryName the query name
   * @return the string
   * @throws UnsupportedEncodingException the unsupported encoding exception
   * @throws LensAPIException
   */
  @CliCommand(value = "prepQuery prepare",
    help = "Prepapre query <query-string> and return prepare handle. Can optionaly provide <query-name>")
  public String prepare(@CliOption(key = {"", "query"}, mandatory = true, help = "<query-string>") String sql,
    @CliOption(key = {"name"}, mandatory = false, help = "<query-name>") String queryName)
    throws UnsupportedEncodingException, LensAPIException {
    return getClient().prepare(sql, queryName, new LensConf()).getData().toString();
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
   * @throws LensAPIException
   */
  @CliCommand(value = "prepQuery explain",
    help = "Explain and prepare query <query-string>. Can optionally provide <query-name>")
  public String explainAndPrepare(

    @CliOption(key = {"", "query"}, mandatory = true, help = "<query-string>") String sql,
    @CliOption(key = {"name"}, mandatory = false, help = "<query-name>") String queryName)
    throws UnsupportedEncodingException, LensAPIException {
    PrettyPrintable cliOutput;
    try {
      QueryPlan plan = getClient().explainAndPrepare(sql, queryName, new LensConf()).getData();
      return plan.getPlanString() + "\n" + "Prepare handle:" + plan.getPrepareHandle();
    } catch (final LensAPIException e) {
      BriefError briefError = new BriefError(e.getLensAPIErrorCode(), e.getLensAPIErrorMessage());
      cliOutput = new IdBriefErrorTemplate(IdBriefErrorTemplateKey.REQUEST_ID, e.getLensAPIRequestId(), briefError);
    } catch (final LensBriefErrorException e) {
      cliOutput = e.getIdBriefErrorTemplate();
    }
    return cliOutput.toPrettyString();
  }
}
