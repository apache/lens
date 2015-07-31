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
package org.apache.lens.driver.es.translator;

import static org.apache.lens.driver.es.ESDriverConfig.MATCH_ALL;

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

import java.util.List;

import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.driver.es.ASTTraverserForES;
import org.apache.lens.driver.es.ESDriverConfig;
import org.apache.lens.driver.es.ESQuery;
import org.apache.lens.driver.es.translator.impl.ESAggregateVisitor;
import org.apache.lens.driver.es.translator.impl.ESCriteriaVisitor;
import org.apache.lens.driver.es.translator.impl.ESCriteriaVisitorFactory;
import org.apache.lens.driver.es.translator.impl.ESTermVisitor;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import lombok.NonNull;

/**
 * The abstract visitor for constructing elastic search queries from ASTNode
 */
public abstract class ESVisitor implements ASTVisitor {
  protected static final JsonNodeFactory JSON_NODE_FACTORY = JsonNodeFactory.instance;

  protected final ObjectNode queryNode = JSON_NODE_FACTORY.objectNode();
  protected final List<String> querySchema = Lists.newArrayList();
  protected final List<String> selectedColumnNames = Lists.newArrayList();
  @NonNull
  protected final ESDriverConfig config;
  protected ESQuery.QueryType queryType;

  protected String index;
  protected String type;
  protected int limit;
  protected JsonNode criteriaNode = getMatchAllNode();

  protected ESVisitor(ESDriverConfig config) {
    this.config = config;
    limit = config.getMaxLimit();
  }

  private static JsonNode getMatchAllNode() {
    final ObjectNode matchAllNode = JSON_NODE_FACTORY.objectNode();
    matchAllNode.put(MATCH_ALL, JSON_NODE_FACTORY.objectNode());
    return matchAllNode;
  }

  public static ESQuery rewrite(ESDriverConfig config, String hql) throws LensException {
    ASTNode rootQueryNode;
    try {
      rootQueryNode = HQLParser.parseHQL(hql, new HiveConf());
    } catch (Exception e) {
      throw new ESRewriteException(e);
    }
    return rewrite(config, rootQueryNode);
  }

  public static ESQuery rewrite(ESDriverConfig config, ASTNode rootQueryNode) throws LensException {
    final ESVisitor visitor = isAggregateQuery(rootQueryNode)
      ?
      new ESAggregateVisitor(config)
      :
      new ESTermVisitor(config);

    new ASTTraverserForES(
      rootQueryNode,
      visitor,
      new ESCriteriaVisitorFactory()
    ).accept();

    return visitor.getQuery();
  }

  /**
   * Have to move them to proper util classes
   * @param rootQueryNode, root node of AST
   * @return if the query is aggregate
   */
  private static boolean isAggregateQuery(ASTNode rootQueryNode) {
    return hasGroupBy(rootQueryNode) || areAllColumnsAggregate(rootQueryNode);
  }

  /**
   * Have to move them to proper util functions
   * @param rootQueryNode, root node of AST
   * @return if all Cols are aggregate
   */
  private static boolean areAllColumnsAggregate(ASTNode rootQueryNode) {
    boolean areAllColumnsAggregate = true;
    final ASTNode selectNode = HQLParser.findNodeByPath(rootQueryNode, TOK_INSERT, TOK_SELECT);
    for (Node selectExp : selectNode.getChildren()) {
      final Node innerNode = selectExp.getChildren().get(0);
      if (!innerNode.getName().equals(String.valueOf(TOK_FUNCTION))) {
        areAllColumnsAggregate = false;
        break;
      }
    }
    return areAllColumnsAggregate;
  }

  private static boolean hasGroupBy(ASTNode rootQueryNode) {
    return HQLParser.findNodeByPath(rootQueryNode, TOK_INSERT, TOK_GROUPBY) != null;
  }

  protected static String visitColumn(String cannonicalColName) {
    final String[] colParts = cannonicalColName.split("\\.");
    return colParts[colParts.length - 1];
  }

  @Override
  public void visitFrom(String database, String table) {
    Validate.notNull(database);
    Validate.notNull(table);
    Preconditions.checkNotNull(table);
    index = database;
    type = table;
  }

  @Override
  public void visitLimit(int limit) {
    this.limit = limit;
  }

  @Override
  public void visitCriteria(ASTCriteriaVisitor visitedSubTree) {
    criteriaNode = ((ESCriteriaVisitor) visitedSubTree).getNode();
  }

  @Override
  public void visitAllCols() {
    throw new UnsupportedOperationException("'*' is not supported in elastic search, select the columns required");
  }

  public ESQuery getQuery() {
    Validate.isTrue(querySchema.size() == selectedColumnNames.size(),
      "Query building seems to have gone wrong... schema and alias list size seems to be different");
    return new ESQuery(index, type, queryNode.toString(), ImmutableList.copyOf(querySchema),
      ImmutableList.copyOf(selectedColumnNames), queryType, limit);
  }

  private static class ESRewriteException extends RuntimeException {
    public ESRewriteException(Exception e) {
      super(e);
    }
  }

}
