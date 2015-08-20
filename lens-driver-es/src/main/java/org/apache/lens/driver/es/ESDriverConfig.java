/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.driver.es;

import org.apache.lens.driver.es.translator.ASTVisitor;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;

/**
 * The constants used by ESDriver and es re-writers
 */
public final class ESDriverConfig {
  public static final String CLIENT_CLASS_KEY = "lens.driver.es.client.class";
  public static final String MAX_ROW_SIZE_KEY = "lens.driver.es.max.row.size";
  public static final String TERM_FETCH_SIZE_KEY = "lens.driver.es.term.fetch.size";
  public static final String AGGR_BUCKET_SIZE_LENS_KEY = "lens.driver.es.aggr.bucket.size";
  public static final String QUERY_TIME_OUT_LENS_KEY = "lens.driver.es.query.timeout.millis";

  public static final String AGGS = "aggs";
  public static final String MATCH_ALL = "match_all";
  public static final String TERMS = "terms";
  public static final String TERM = "term";
  public static final String FIELD = "field";
  public static final String FILTER = "filter";
  public static final String FROM = "from";
  public static final String RANGE = "range";
  public static final String FILTER_WRAPPER = "filter_wrapper";
  public static final String FIELDS = "fields";
  public static final String TERM_SORT = "sort";
  public static final String SIZE = "size";
  public static final String QUERY_TIME_OUT_STRING = "timeout";

  public static final ImmutableMap<ASTVisitor.OrderBy, String> ORDER_BYS;
  public static final int AGGR_TERM_FETCH_SIZE = 0;
  public static final int DEFAULT_TERM_QUERY_OFFSET = 0;
  private static final int DEFAULT_TERM_QUERY_LIMIT = -1;
  private static final int AGGR_BUCKET_SIZE_DEFAULT = 10000;
  private static final int QUERY_TIME_OUT_MS_DEFAULT = 10000;

  private static final int TERM_FETCH_SIZE_DEFAULT = 5000;

  static {
    final ImmutableMap.Builder<ASTVisitor.OrderBy, String> orderByBuilder = ImmutableMap.builder();
    orderByBuilder.put(ASTVisitor.OrderBy.ASC, "asc");
    orderByBuilder.put(ASTVisitor.OrderBy.DESC, "desc");
    ORDER_BYS = orderByBuilder.build();
  }

  @Getter
  private final int maxLimit;
  @Getter
  private final int aggrBucketSize;
  @Getter
  private final int queryTimeOutMs;
  private final int termFetchSize;

  public int getTermFetchSize() {
    return termFetchSize;
  }

  public ESDriverConfig(Configuration conf) {
    maxLimit = conf.getInt(MAX_ROW_SIZE_KEY, DEFAULT_TERM_QUERY_LIMIT);
    aggrBucketSize = conf.getInt(AGGR_BUCKET_SIZE_LENS_KEY, AGGR_BUCKET_SIZE_DEFAULT);
    queryTimeOutMs = conf.getInt(QUERY_TIME_OUT_LENS_KEY, QUERY_TIME_OUT_MS_DEFAULT);
    termFetchSize = conf.getInt(TERM_FETCH_SIZE_KEY, TERM_FETCH_SIZE_DEFAULT);
  }


}
