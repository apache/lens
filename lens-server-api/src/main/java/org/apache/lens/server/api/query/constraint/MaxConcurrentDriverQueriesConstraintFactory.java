/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lens.server.api.query.constraint;

import static org.apache.lens.api.util.CommonUtils.parseMapFromString;

import java.util.Map;

import org.apache.lens.api.Priority;
import org.apache.lens.api.util.CommonUtils.EntryParser;
import org.apache.lens.server.api.common.ConfigBasedObjectCreationFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

public class MaxConcurrentDriverQueriesConstraintFactory
  implements ConfigBasedObjectCreationFactory<MaxConcurrentDriverQueriesConstraint> {

  public static final String MAX_CONCURRENT_QUERIES_KEY = "driver.max.concurrent.launched.queries";
  private static final String PREFIX = MAX_CONCURRENT_QUERIES_KEY + ".per.";
  public static final String MAX_CONCURRENT_QUERIES_PER_QUEUE_KEY = PREFIX + "queue";
  public static final String MAX_CONCURRENT_QUERIES_PER_PRIORITY_KEY = PREFIX + "priority";
  private static final EntryParser<String, Integer> STRING_INT_PARSER = new EntryParser<String, Integer>() {
    @Override
    public String parseKey(String str) {
      return str;
    }

    @Override
    public Integer parseValue(String str) {
      return Integer.valueOf(str);
    }
  };
  private static final EntryParser<Priority, Integer> PRIORITY_INT_PARSER = new EntryParser<Priority, Integer>() {
    @Override
    public Priority parseKey(String str) {
      return Priority.valueOf(str.toUpperCase());
    }

    @Override
    public Integer parseValue(String str) {
      return Integer.valueOf(str);
    }
  };

  @Override
  public MaxConcurrentDriverQueriesConstraint create(final Configuration conf) {
    String maxConcurrentQueriesValue = conf.get(MAX_CONCURRENT_QUERIES_KEY);
    Map<String, Integer> maxConcurrentQueriesPerQueue = parseMapFromString(
      conf.get(MAX_CONCURRENT_QUERIES_PER_QUEUE_KEY), STRING_INT_PARSER);
    Map<Priority, Integer> maxConcurrentQueriesPerPriority = parseMapFromString(
      conf.get(MAX_CONCURRENT_QUERIES_PER_PRIORITY_KEY), PRIORITY_INT_PARSER);
    int maxConcurrentQueries = Integer.MAX_VALUE;
    if (!StringUtils.isBlank(maxConcurrentQueriesValue)) {
      maxConcurrentQueries = Integer.parseInt(maxConcurrentQueriesValue);
    }
    return new MaxConcurrentDriverQueriesConstraint(maxConcurrentQueries, maxConcurrentQueriesPerQueue,
      maxConcurrentQueriesPerPriority);

  }
}
