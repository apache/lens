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

package org.apache.lens.cube.parse;

import java.util.*;

import org.apache.lens.cube.metadata.FactPartition;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Collapses the time range filters using IN operators
 */
public class AbridgedTimeRangeWriter implements TimeRangeWriter {
  //TODO: minimize use of String, use StringBuilders

  /**
   * Return IN clause for the partitions selected in the cube query
   *
   * @param cubeQueryContext
   * @param tableName
   * @param parts
   * @return
   * @throws SemanticException
   */
  @Override
  public String getTimeRangeWhereClause(CubeQueryContext cubeQueryContext,
    String tableName,
    Set<FactPartition> parts) throws SemanticException {
    if (parts == null || parts.isEmpty()) {
      return "";
    }
    // Collect partition specs by column in a map
    // All filters which contain only a single column will be combined in an IN operator clause
    // This clause will be ORed with filters which contain multiple columns.
    List<String> subFilters = new ArrayList<String>();
    for (Map.Entry<Set<FactPartition>, Set<FactPartition>> entry : groupPartitions(parts).entrySet()) {
      List<String> clauses = new ArrayList<String>();
      String clause;
      clause = getClause(cubeQueryContext, tableName, entry.getKey());
      if (clause != null && !clause.isEmpty()) {
        clauses.add(clause);
      }
      clause = getClause(cubeQueryContext, tableName, entry.getValue());
      if (clause != null && !clause.isEmpty()) {
        clauses.add(clause);
      }
      subFilters.add("(" + StringUtils.join(clauses, " AND ") + ")");
    }
    return StringUtils.join(subFilters, " OR ");
  }

  private String getClause(CubeQueryContext cubeQueryContext,
    String tableName,
    Set<FactPartition> parts) throws SemanticException {
    Map<String, List<String>> partFilterMap = new HashMap<String, List<String>>();
    List<String> allTimeRangeFilters = new ArrayList<String>();

    for (FactPartition factPartition : parts) {
      String filter = TimeRangeUtils.getTimeRangePartitionFilter(factPartition, cubeQueryContext, tableName);
      if (filter.contains("AND")) {
        allTimeRangeFilters.add(new StringBuilder("(").append(filter).append(")").toString());
      } else {
        extractColumnAndCondition(filter, partFilterMap);
      }
    }

    List<String> inClauses = new ArrayList<String>(partFilterMap.size());
    for (String column : partFilterMap.keySet()) {
      String clause =
        new StringBuilder("(").append(StringUtils.join(partFilterMap.get(column), ",")).append(")").toString();
      inClauses.add(column + " IN " + clause);
    }

    allTimeRangeFilters.add(StringUtils.join(inClauses, " AND "));
    return StringUtils.join(allTimeRangeFilters, " OR ");
  }

  /**
   * parts is a collection of FactPartition objects. And FactPartition can be viewed as two boolean conditions, one
   * specified by it's containingPart object, and another specified by itself in the form (partCol = partSpec)
   * <p/>
   * Collection of FactPartition objects can be viewed as an OR clause on all the FactPartition objects -- which by
   * itself is a binary AND clause.
   * <p/>
   * So Collection&lt;FactPartition&gt; is nothing but (a AND b) OR (c AND d) OR (e AND f) ...
   * <p/>
   * This function tries to reduce such a big clause by using Boolean arithmetic. The big thing it aims to reduce is the
   * following class of clauses:
   * <p/>
   * (a AND c) OR (a AND d) OR (b AND c) OR (b AND d) => ((a OR b) AND (c OR d))
   * <p/>
   * Equivalent return value for such a reduction would be an entry in the returned map from set(a,b) to set(c,d).
   * Assuming the argument was set(a(containing=c), a(containing=d), b(containing=c), b(containing=d))
   *
   * @param parts
   * @return
   */
  private Map<Set<FactPartition>, Set<FactPartition>> groupPartitions(Collection<FactPartition> parts) {
    Map<FactPartition, Set<FactPartition>> partitionSetMap = new HashMap<FactPartition, Set<FactPartition>>();
    for (FactPartition part : parts) {
      FactPartition key = part.getContainingPart();
      part.setContainingPart(null);
      if (partitionSetMap.get(key) == null) {
        partitionSetMap.put(key, Sets.<FactPartition>newTreeSet());
      }
      partitionSetMap.get(key).add(part);
    }
    Map<Set<FactPartition>, Set<FactPartition>> setSetOppositeMap = Maps.newHashMap();
    for (Map.Entry<FactPartition, Set<FactPartition>> entry : partitionSetMap.entrySet()) {
      if (setSetOppositeMap.get(entry.getValue()) == null) {
        setSetOppositeMap.put(entry.getValue(), Sets.<FactPartition>newTreeSet());
      }
      if (entry.getKey() != null) {
        setSetOppositeMap.get(entry.getValue()).add(entry.getKey());
      }
    }

    Map<Set<FactPartition>, Set<FactPartition>> setSetMap = Maps.newHashMap();
    for (Map.Entry<Set<FactPartition>, Set<FactPartition>> entry : setSetOppositeMap.entrySet()) {
      setSetMap.put(entry.getValue(), entry.getKey());
    }
    return setSetMap;
  }

  // This takes the output of filter generated by TimeRangeUtils.getTimeRangePartitionFilter
  // splits the filters by column names and filters are collected by column name in the
  // map passed as argument
  private void extractColumnAndCondition(String token, Map<String, List<String>> partFilterMap) {
    token = token.trim();

    String[] subTokens = StringUtils.split(token, '=');

    String column = subTokens[0].trim();
    String filterValue = subTokens[1].trim();

    List<String> filterValues = partFilterMap.get(column);

    if (filterValues == null) {
      filterValues = new ArrayList<String>();
      partFilterMap.put(column, filterValues);
    }

    filterValues.add(filterValue);
  }
}
