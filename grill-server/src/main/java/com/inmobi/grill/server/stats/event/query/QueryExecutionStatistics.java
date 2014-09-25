package com.inmobi.grill.server.stats.event.query;
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

import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.lib.query.JSonSerde;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.stats.event.LoggableGrillStatistics;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.mapred.TextInputFormat;

import java.util.LinkedList;

/**
 * Statistics class used to capture query information.
 */
public class QueryExecutionStatistics extends LoggableGrillStatistics {
  @Getter
  @Setter
  private String handle;

  @Getter
  @Setter
  private String userQuery;

  @Getter
  @Setter
  private String submitter;

  @Getter
  @Setter
  private String clusterUser;

  @Getter
  @Setter
  private String sessionId;

  @Getter
  @Setter
  private long submissionTime;


  @Getter
  @Setter
  private long startTime;

  @Getter
  @Setter
  private long endTime;


  @Getter
  @Setter
  private String result;


  @Getter
  @Setter
  private QueryStatus status;

  @Getter
  @Setter
  private String cause;

  @Getter
  @Setter
  private QueryDriverStatistics driverStats;

  //Used while reflection to create hive table.
  public QueryExecutionStatistics() {
    super();
  }

  public QueryExecutionStatistics(long eventTime) {
    super(eventTime);
  }

  @Override
  public Table getHiveTable(Configuration conf) {
    Table table = new Table(conf.get(GrillConfConstants.GRILL_STATISTICS_DATABASE_KEY,
        GrillConfConstants.DEFAULT_STATISTICS_DATABASE),this.getClass().getSimpleName());
    LinkedList<FieldSchema> colList = new LinkedList<FieldSchema>();
    colList.add(new FieldSchema("handle", "string", "Query Handle"));
    colList.add(new FieldSchema("userQuery", "string", "User Query before rewrite"));
    colList.add(new FieldSchema("submitter", "string", "submitter"));
    colList.add(new FieldSchema("clusterUser", "string", "Cluster User which will do all operations on hdfs"));
    colList.add(new FieldSchema("sessionId","string", "Grill Session which ran the query"));
    colList.add(new FieldSchema("submissionTime", "bigint", "Time which query was submitted"));
    colList.add(new FieldSchema("startTime", "bigint", "Timestamp which query was Started"));
    colList.add(new FieldSchema("endTime", "bigint", "Timestamp which query was finished"));
    colList.add(new FieldSchema("result", "string", "path to result of query"));
    colList.add(new FieldSchema("cause", "string", "failure/eror cause if any"));
    colList.add(new FieldSchema("status", "map<string,string>","status object of the query"));
    colList.add(new FieldSchema("driverStats", "map<string,string>", "driver statistics of the query"));
    table.setFields(colList);
    LinkedList<FieldSchema> partCols = new LinkedList<FieldSchema>();
    partCols.add(new FieldSchema("dt", "string", "partCol"));
    table.setPartCols(partCols);
    table.setSerializationLib(JSonSerde.class.getName());
    try {
      table.setInputFormatClass(TextInputFormat.class.getName());
    } catch (HiveException e) {
      e.printStackTrace();
    }
    return table;
  }


  @Override
  public String getEventId() {
    return "queryExecutionStatistics";
  }
}
