/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.driver.es.grammar;

import org.apache.lens.driver.es.ESDriverConfig;
import org.apache.lens.driver.es.exceptions.InvalidQueryException;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;

public enum Aggregations {
  value_count,
  cardinality,
  max,
  sum,
  min,
  avg;

  protected static final JsonNodeFactory JSON_NODE_FACTORY = JsonNodeFactory.instance;

  public void build(ObjectNode targetAggNode, String columnName, String alias) {
    final ObjectNode aggMeasures = JSON_NODE_FACTORY.objectNode();
    final ObjectNode fieldNode = JSON_NODE_FACTORY.objectNode();
    fieldNode.put(ESDriverConfig.FIELD, columnName);
    targetAggNode.put(alias, aggMeasures);
    aggMeasures.put(String.valueOf(this), fieldNode);
  }

  public static Aggregations getFor(String hqlAggregationName) throws InvalidQueryException {
    if (HQL_AF_MAP.containsKey(hqlAggregationName)) {
      return HQL_AF_MAP.get(hqlAggregationName);
    }
    throw new InvalidQueryException("No handlers registered for aggregation " + hqlAggregationName);
  }

  private static final ImmutableMap<String, Aggregations> HQL_AF_MAP;
  static {
    final ImmutableMap.Builder<String, Aggregations> hqlAFMapBuilder = ImmutableMap.builder();
    hqlAFMapBuilder.put("count", value_count);
    hqlAFMapBuilder.put("count_distinct", cardinality);
    hqlAFMapBuilder.put("max", max);
    hqlAFMapBuilder.put("sum", sum);
    hqlAFMapBuilder.put("min", min);
    hqlAFMapBuilder.put("avg", avg);
    HQL_AF_MAP = hqlAFMapBuilder.build();
  }
}
