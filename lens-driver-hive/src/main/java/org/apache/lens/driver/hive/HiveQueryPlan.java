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
package org.apache.lens.driver.hive;

import java.util.*;

import org.apache.lens.api.query.QueryPrepareHandle;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.DriverQueryPlan;
import org.apache.lens.server.api.query.cost.QueryCost;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * The Class HiveQueryPlan.
 */
@Slf4j
public class HiveQueryPlan extends DriverQueryPlan {

  @Getter
  private final QueryCost cost;
  /** The explain output. */
  private String explainOutput;

  /**
   * The Enum ParserState.
   */
  enum ParserState {

    /** The begin. */
    BEGIN,

    /** The file output operator. */
    FILE_OUTPUT_OPERATOR,

    /** The table scan. */
    TABLE_SCAN,

    /** The join. */
    JOIN,

    /** The select. */
    SELECT,

    /** The groupby. */
    GROUPBY,

    /** The groupby keys. */
    GROUPBY_KEYS,

    /** The groupby exprs. */
    GROUPBY_EXPRS,

    /** The move. */
    MOVE,

    /** The map reduce. */
    MAP_REDUCE,

    /** The partition list. */
    PARTITION_LIST,

    /** The partition. */
    PARTITION,

    /** CREATE TABLE if destination is a table */
    CREATE
  }

  /**
   * Instantiates a new hive query plan.
   *
   * @param explainOutput the explain output
   * @param prepared      the prepared
   * @param metastoreConf the metastore conf
   * @throws HiveException the hive exception
   */
  public HiveQueryPlan(List<String> explainOutput, QueryPrepareHandle prepared, HiveConf metastoreConf, QueryCost cost)
    throws HiveException {
    this.cost = cost;
    setPrepareHandle(prepared);
    setExecMode(ExecMode.BATCH);
    setScanMode(ScanMode.PARTIAL_SCAN);
    this.explainOutput = StringUtils.join(explainOutput, '\n');
    extractPlanDetails(explainOutput, metastoreConf);
  }

  /**
   * Extract plan details.
   *
   * @param explainOutput the explain output
   * @param metastoreConf the metastore conf
   * @throws HiveException the hive exception
   */
  private void extractPlanDetails(List<String> explainOutput, HiveConf metastoreConf) throws HiveException {
    ParserState state = ParserState.BEGIN;
    ParserState prevState = state;
    ArrayList<ParserState> states = new ArrayList<ParserState>();
    Hive metastore = Hive.get(metastoreConf);

    for (int i = 0; i < explainOutput.size(); i++) {
      String line = explainOutput.get(i);
      String tr = line.trim();
      prevState = state;
      state = nextState(tr, state);

      if (prevState != state) {
        states.add(prevState);
      }

      switch (state) {
      case MOVE:
        if (tr.startsWith("destination:")) {
          String outputPath = tr.replace("destination:", "").trim();
          resultDestination = outputPath;
        }
        break;
      case TABLE_SCAN:
        int indentation = getIndentation(explainOutput.get(i));
        i++;
        for (; i < explainOutput.size(); i++) {
          if (explainOutput.get(i).trim().startsWith("alias: ")) {
            String tableName = explainOutput.get(i).trim().substring(7);
            if (!tableName.contains(".") && SessionState.get() != null) {
              tableName = SessionState.get().getCurrentDatabase() + "." + tableName;
            }
            addTableToTablesQueried(tableName, metastore);
          }
          if (i + 1 < explainOutput.size() && getIndentation(explainOutput.get(i+1)) <= indentation) {
            break;
          }
        }
        break;
      case PARTITION:
        String partConditionStr = null;
        for (; i < explainOutput.size(); i++) {
          if (explainOutput.get(i).trim().equals("partition values:")) {
            List<String> partVals = new ArrayList<String>();
            // Look ahead until we reach partition properties
            String lineAhead = null;
            for (; i < explainOutput.size(); i++) {
              if (explainOutput.get(i).trim().equals("properties:")) {
                break;
              }
              lineAhead = explainOutput.get(i).trim();
              partVals.add(lineAhead);
            }

            partConditionStr = StringUtils.join(partVals, ";");
          }
          // Now seek table name
          if (explainOutput.get(i).trim().startsWith("name:")) {
            String table = explainOutput.get(i).trim().substring("name:".length()).trim();
            // update tables queried and weights
            addTableToTablesQueried(table, metastore);

            if (partConditionStr != null) {
              Set<String> tablePartitions = (Set<String>) partitions.get(table);
              if (tablePartitions == null) {
                tablePartitions = new HashSet<String>();
                partitions.put(table, tablePartitions);
              }
              tablePartitions.add(partConditionStr);
            }
            break;
          }
          if (explainOutput.get(i).trim().startsWith("Stage: ")) {
            // stage got changed
            break;
          }
        }
        break;
      default:
        break;
      }
    }
  }

  private void addTableToTablesQueried(String table, Hive metastore) throws HiveException {
    if (!tablesQueried.contains(table)) {
      Table tbl = metastore.getTable(table, false);
      if (tbl == null) {
        // table not found, possible case if query is create table
        log.info("Table {} not found while extracting plan details", table);
        return;
      }
      tablesQueried.add(table);
      String costStr = tbl.getParameters().get(LensConfConstants.STORAGE_COST);

      Double weight = 1d;
      if (costStr != null) {
        weight = Double.parseDouble(costStr);
      }
      tableWeights.put(table, weight);
    }
  }

  private int getIndentation(String s) {
    for(int i = 0; i < s.length(); i++) {
      if (s.charAt(i) != ' ') {
        return i - 1;
      }
    }
    return s.length();
  }

  /**
   * Next state.
   *
   * @param tr    the tr
   * @param state the state
   * @return the parser state
   */
  private ParserState nextState(String tr, ParserState state) {
    if (tr.equals("File Output Operator")) {
      return ParserState.FILE_OUTPUT_OPERATOR;
    } else if (tr.equals("Map Reduce")) {
      return ParserState.MAP_REDUCE;
    } else if (tr.equals("Move Operator")) {
      return ParserState.MOVE;
    } else if (tr.equals("TableScan")) {
      return ParserState.TABLE_SCAN;
    } else if (tr.equals("Map Join Operator")) {
      return ParserState.JOIN;
    } else if (tr.equals("Select Operator")) {
      return ParserState.SELECT;
    } else if (tr.equals("Group By Operator")) {
      return ParserState.GROUPBY;
    } else if (tr.startsWith("aggregations:") && state == ParserState.GROUPBY) {
      return ParserState.GROUPBY_EXPRS;
    } else if (tr.startsWith("keys:") && state == ParserState.GROUPBY_EXPRS) {
      return ParserState.GROUPBY_KEYS;
    } else if (tr.equals("Path -> Partition:") || tr.equals("Partition Description:")) {
      return ParserState.PARTITION_LIST;
    } else if (tr.equals("Partition") && state == ParserState.PARTITION_LIST) {
      return ParserState.PARTITION;
    } else if (tr.equals("Create Table Operator")) {
      return ParserState.CREATE;
    }
    return state;
  }

  @Override
  public String getPlan() {
    return explainOutput;
  }
}
