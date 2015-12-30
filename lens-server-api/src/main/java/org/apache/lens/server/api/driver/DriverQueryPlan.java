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
package org.apache.lens.server.api.driver;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;

import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryPlan;
import org.apache.lens.api.query.QueryPrepareHandle;
import org.apache.lens.server.api.query.cost.QueryCost;
import org.apache.lens.server.api.query.cost.QueryCostTOBuilder;

import lombok.extern.slf4j.Slf4j;

/**
 * The Class DriverQueryPlan.
 */
@Slf4j
public abstract class DriverQueryPlan {

  /**
   * The Enum ExecMode.
   */
  public enum ExecMode {

    /**
     * The interactive.
     */
    INTERACTIVE,

    /**
     * The batch.
     */
    BATCH,

    /**
     * The not accepted.
     */
    NOT_ACCEPTED
  }

  /**
   * The Enum ScanMode.
   */
  public enum ScanMode {

    /**
     * The in memory.
     */
    IN_MEMORY,

    /**
     * The index scan.
     */
    INDEX_SCAN,

    /**
     * The partial scan.
     */
    PARTIAL_SCAN,

    /**
     * The full scan.
     */
    FULL_SCAN
  }

  /**
   * The tables queried.
   */
  protected final Set<String> tablesQueried = new HashSet<String>();

  /**
   * The has sub query.
   */
  protected boolean hasSubQuery = false;

  /**
   * The result destination.
   */
  protected String resultDestination;

  /**
   * The exec mode.
   */
  protected ExecMode execMode;

  /**
   * The scan mode.
   */
  protected ScanMode scanMode;

  /**
   * The table weights.
   */
  protected final Map<String, Double> tableWeights = new HashMap<String, Double>();

  /**
   * The handle.
   */
  protected QueryPrepareHandle handle;

  protected Map<String, Set<?>> partitions = new HashMap<String, Set<?>>();

  /**
   * Get the query plan
   *
   * @return The string representation of the plan
   */
  public abstract String getPlan();

  /**
   * Get the cost associated with the plan
   *
   * @return QueryCostTO object
   */
  public abstract QueryCost getCost();

  /**
   * Get the list of tables to be queried
   *
   * @return the tablesQueried
   */
  public Set<String> getTablesQueried() {
    return tablesQueried;
  }

  /**
   * Set the list of table names to be queried.
   *
   * @param table the table
   */
  protected void addTablesQueried(String table) {
    this.tablesQueried.add(table);
  }

  /**
   * Set the list of table names to be queried.
   *
   * @param tables the table
   */
  protected void addTablesQueried(Set<String> tables) {
    this.tablesQueried.addAll(tables);
  }

  /**
   * Get if the query has a subquery or not.
   *
   * @return the hasSubQuery true if query has subquery, false otherwise
   */
  public boolean hasSubQuery() {
    return hasSubQuery;
  }

  /**
   * Set if query has subquery.
   */
  protected void setHasSubQuery(boolean hasSubQuery) {
    this.hasSubQuery = hasSubQuery;
  }

  /**
   * Get the result destination
   *
   * @return the resultDestination The destination can be another table or filesystem path or inmemory result
   */
  public String getResultDestination() {
    return resultDestination;
  }

  /**
   * Set string representation of the destination
   *
   * @param resultDestination the resultDestination to set
   */
  protected void setResultDestination(String resultDestination) {
    this.resultDestination = resultDestination;
  }

  /**
   * Get the table weights
   *
   * @return the tableWeights
   */
  public Map<String, Double> getTableWeights() {
    return tableWeights;
  }

  /**
   * Get the weight of the table.
   *
   * @param tableName the table name
   * @return the weight
   */
  public Double getTableWeight(String tableName) {
    return tableWeights.get(tableName);
  }

  /**
   * Set the weight of the table.
   *
   * @param tableName   The name of the table.
   * @param tableWeight Weight of the table being queried.
   *                    This should reflect the amount of data being read/scanned from the table, scan cost
   */
  protected void setTableWeight(String tableName, Double tableWeight) {
    this.tableWeights.put(tableName, tableWeight);
  }

  /**
   * Get the exec mode
   *
   * @return the {@link ExecMode}
   */
  public ExecMode getExecMode() {
    return execMode;
  }

  /**
   * Set the exec mode
   *
   * @param execMode the {@link ExecMode} to set
   */
  protected void setExecMode(ExecMode execMode) {
    this.execMode = execMode;
  }

  /**
   * Get the scan mode.
   *
   * @return the {@link ScanMode}
   */
  public ScanMode getScanMode() {
    return scanMode;
  }

  /**
   * Set the scan mode
   *
   * @param scanMode the {@link ScanMode} to set
   */
  protected void setScanMode(ScanMode scanMode) {
    this.scanMode = scanMode;
  }

  /**
   * @return the handle
   * @deprecated
   */
  public QueryHandle getHandle() {
    if (handle != null) {
      return new QueryHandle(handle.getPrepareHandleId());
    } else {
      return null;
    }
  }

  /**
   * @return the prepare handle
   */
  public QueryPrepareHandle getPrepareHandle() {
    return handle;
  }

  /**
   * @param handle the handle to set
   */
  public void setPrepareHandle(QueryPrepareHandle handle) {
    this.handle = handle;
  }

  /**
   * Get list of partitions queried for each table
   *
   * @return
   */
  public Map<String, Set<?>> getPartitions() {
    return partitions;
  }

  /**
   * To query plan.
   *
   * @return the query plan
   * @throws UnsupportedEncodingException the unsupported encoding exception
   */
  public QueryPlan toQueryPlan() throws UnsupportedEncodingException {
    return new QueryPlan(new ArrayList<>(tablesQueried), hasSubQuery, execMode != null ? execMode.name() : null,
      scanMode != null ? scanMode.name() : null, handle,
      URLEncoder.encode(getPlan(), "UTF-8"), new QueryCostTOBuilder(getCost()).build());
  }
}
