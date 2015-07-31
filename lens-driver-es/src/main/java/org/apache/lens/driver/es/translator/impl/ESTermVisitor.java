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

import static org.apache.lens.driver.es.ESDriverConfig.*;

import org.apache.lens.driver.es.ESDriverConfig;
import org.apache.lens.driver.es.ESQuery;
import org.apache.lens.driver.es.translator.ESVisitor;

import org.apache.commons.lang3.Validate;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The ESTermVisitor for constructing elastic search queries from ASTNode
 */
public class ESTermVisitor extends ESVisitor {

  final ArrayNode fields = JSON_NODE_FACTORY.arrayNode();
  final ArrayNode sorts = JSON_NODE_FACTORY.arrayNode();

  public ESTermVisitor(ESDriverConfig config) {
    super(config);
    queryType = ESQuery.QueryType.TERM;
  }

  @Override
  public void visitSimpleSelect(String column, String alias) {
    column = visitColumn(column);
    final String aliasName = alias == null ? column : alias;
    Validate.isTrue(!querySchema.contains(aliasName), "Ambiguous alias '" + aliasName + "'");
    querySchema.add(aliasName);
    selectedColumnNames.add(column);
    fields.add(column);
  }

  @Override
  public void visitAggregation(String aggregationType, String columnName, String alias) {
    throw new UnsupportedOperationException("Valid groups have to be specified for aggregation");
  }

  @Override
  public void visitGroupBy(String colName) {
    throw new UnsupportedOperationException("Group bys are not specified in a term query");
  }

  @Override
  public void visitLimit(int limit) {
    if (this.limit==-1 || limit < config.getMaxLimit()) {
      this.limit = limit;
    }
  }

  @Override
  public void visitOrderBy(String colName, OrderBy orderBy) {
    colName = visitColumn(colName);
    final ObjectNode sortNode = JSON_NODE_FACTORY.objectNode();
    sortNode.put(colName, ORDER_BYS.get(orderBy));
    sorts.add(sortNode);
  }

  @Override
  public void completeVisit() {
    queryNode.put(FROM, DEFAULT_TERM_QUERY_OFFSET);
    final int initialBatch = limit != -1 && limit < config.getTermFetchSize()
      ?
      limit
      :
      config.getTermFetchSize();
    queryNode.put(SIZE, initialBatch);
    queryNode.put(FIELDS, fields);
    queryNode.put(TERM_SORT, sorts);
    queryNode.put(ESDriverConfig.QUERY_TIME_OUT_STRING, config.getQueryTimeOutMs());
    queryNode.put(FILTER, criteriaNode);
  }
}
