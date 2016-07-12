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

package org.apache.lens.regression.core.constants;

import java.util.Properties;

import org.apache.lens.regression.util.Util;


public class QueryInventory {

  static final String QUERIES_PROPERTIES = "queries.properties";
  static Properties queryMap = null;

  static {
    queryMap = Util.getPropertiesObj(QUERIES_PROPERTIES);
  }

  public static String getQueryFromInventory(String queryName){
    return queryMap.get(queryName).toString();
  }


  private QueryInventory() {
  }

  public static String getSleepQuery(String time){
    String query = String.format(SLEEP_QUERY_TIME, time);
    return query;
  }

  public static final String QUERY = "cube select id,name from sample_dim where name != 'first'";
  public static final String WRONG_QUERY = "cube select NO_ID from sample_dim where name != 'first'";

  public static final String DIM_QUERY = "cube select id,name from sample_dim where name != 'first'";
  public static final String CUBE_QUERY = "cube select sample_dim_chain.name, measure4 from sample_cube where "
    + "time_range_in(dt, '2014-06-24-23', '2014-06-25-00')";
  public static final String WRONG_DIM_QUERY = "cube select NO_ID from sample_dim where name != 'first'";

  public static final String HIVE_DIM_QUERY = "cube select id,name from sample_dim2 where name != 'first'";
  public static final String HIVE_CUBE_QUERY = "cube select sample_dim_chain.name, measure4 from sample_cube where "
    + "time_range_in(dt, '2014-06-24-23', '2014-06-25-00')";

  public static final String JDBC_CUBE_QUERY = "cube select product_id from sales where "
    + "time_range_in(delivery_time,'2015-04-12','2015-04-13')";
  public static final String WRONG_JDBC_CUBE_QUERY = "cube select product_id from sales where "
    + "time_range_in(delivery_time,'2015-04-12','2015-04-14')";

  public static final String WRONG_HIVE_DIM_QUERY = "cube select NO_ID from sample_dim2 where name != 'first'";
  public static final String NO_PARTITION_HIVE_CUBE_QUERY="cube select sample_dim_chain.name, measure4 from sample_cube"
    + " where time_range_in(dt, '2014-07-01-00', '2014-07-25-05')";

  public static final String WRONG_SYNTAX_QUERY="cube select id,name from sample_dim2 name != 'first'";

  public static final String JDBC_DIM_QUERY = "cube select id,name from sample_db_dim where name != 'first'";
  public static final String WRONG_JDBC_DIM_QUERY = "cube select NO_ID from sample_db_dim where name != 'first'";

  public static final String SLEEP_FUNCTION = "CREATE TEMPORARY FUNCTION sleep AS 'hive.udf.SampleUdf'";
  public static final String SLEEP_QUERY = "cube select sleep(name) from sample_dim where name != 'first'";
  public static final String SLEEP_QUERY_TIME = "cube select sample_dim_chain.name, sleepTime(measure4,%s) from "
      + "sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-25-00')";

  public static final String NO_CUBE_KEYWORD_QUERY = "select sample_dim_chain.name, measure4 from sample_cube where "
      + "time_range_in(dt, '2014-06-24-23', '2014-06-25-00')";

  public static final String QUOTE_QUERY = "cube select id,name from sample_dim2 where name != 'first\\'s'";

}


