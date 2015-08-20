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
package org.apache.lens.driver.es.translator.impl;

import java.util.Map;

import org.apache.lens.driver.es.ESDriverConfig;
import org.apache.lens.driver.es.ESQuery;
import org.apache.lens.driver.es.exceptions.InvalidQueryException;
import org.apache.lens.driver.es.grammar.Aggregations;
import org.apache.lens.driver.es.translator.ESVisitor;

import org.apache.commons.lang3.Validate;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;

/**
 * Visitor for traversing aggregate elastic search queries
 * that involve group by or simple aggregations
 */
public class ESAggregateVisitor extends ESVisitor {

  private final ObjectNode groupByNode = JSON_NODE_FACTORY.objectNode();
  private final ObjectNode aggNode = JSON_NODE_FACTORY.objectNode();
  private ObjectNode currentGroupByNode = groupByNode;
  private Map<String, String> groupByKeys = Maps.newHashMap();

  public ESAggregateVisitor(ESDriverConfig config) {
    super(config);
    queryType = ESQuery.QueryType.AGGR;
  }

  @Override
  public void visitSimpleSelect(String columnName, String alias) {
    columnName = visitColumn(columnName);
    final String aliasName = alias == null ? columnName : alias;
    Validate.isTrue(!querySchema.contains(aliasName), "Ambiguous alias '" + aliasName + "'");
    querySchema.add(aliasName);
    selectedColumnNames.add(columnName);
    groupByKeys.put(columnName, aliasName);
  }

  @Override
  public void visitAggregation(String aggregationType, String columnName, String alias) throws InvalidQueryException {
    columnName = visitColumn(columnName);
    final String aliasName = alias == null ? columnName : alias;
    querySchema.add(aliasName);
    selectedColumnNames.add(columnName);
    Aggregations.getFor(aggregationType)
      .build(aggNode, columnName, aliasName);
  }

  @Override
  public void visitGroupBy(String groupBy) {
    groupBy = visitColumn(groupBy);
    final ObjectNode aggNode = JSON_NODE_FACTORY.objectNode();
    currentGroupByNode.put(ESDriverConfig.AGGS, aggNode);
    final ObjectNode groupByNode = JSON_NODE_FACTORY.objectNode();
    aggNode.put(
      Validate.notNull(groupByKeys.get(groupBy), "Group by column has to be used in select")
      , groupByNode);
    final ObjectNode termsNode = JSON_NODE_FACTORY.objectNode();
    groupByNode.put(ESDriverConfig.TERMS, termsNode);
    termsNode.put(ESDriverConfig.FIELD, groupBy);
    termsNode.put(ESDriverConfig.SIZE, config.getAggrBucketSize());
    currentGroupByNode = groupByNode;
  }

  @Override
  public void visitOrderBy(String colName, OrderBy orderBy) {
    /**
     * TODO, the feature is partially available in elasticsearch.
     * http://tinyurl.com/p6e5upo
     */
    throw new UnsupportedOperationException("Order by cannot be used with aggregate queries");
  }

  @Override
  public void completeVisit() {
    queryNode.put(ESDriverConfig.SIZE, ESDriverConfig.AGGR_TERM_FETCH_SIZE);
    queryNode.put(ESDriverConfig.QUERY_TIME_OUT_STRING, config.getQueryTimeOutMs());
    final ObjectNode outerAggsNode = JSON_NODE_FACTORY.objectNode();
    queryNode.put(ESDriverConfig.AGGS, outerAggsNode);
    outerAggsNode.put(ESDriverConfig.FILTER_WRAPPER, groupByNode);
    groupByNode.put(ESDriverConfig.FILTER, criteriaNode);
    currentGroupByNode.put(ESDriverConfig.AGGS, aggNode);
  }
}
