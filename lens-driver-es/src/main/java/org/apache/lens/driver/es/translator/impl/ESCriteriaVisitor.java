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

import java.util.List;

import org.apache.lens.driver.es.ESDriverConfig;
import org.apache.lens.driver.es.exceptions.InvalidQueryException;
import org.apache.lens.driver.es.translator.ASTCriteriaVisitor;

import org.apache.commons.lang.Validate;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;


public final class ESCriteriaVisitor implements ASTCriteriaVisitor {

  private final JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;

  private ObjectNode node = jsonNodeFactory.objectNode();

  @Override
  public void visitLogicalOp(String logicalOp, List<ASTCriteriaVisitor> visitedSubTrees) {
    final ArrayNode subTrees = jsonNodeFactory.arrayNode();
    for (ASTCriteriaVisitor criteriaVisitor : visitedSubTrees) {
      subTrees.add(((ESCriteriaVisitor) criteriaVisitor).node);
    }
    node.put(ESDriverConfig.LOGICAL_OPS.get(logicalOp), subTrees);
  }

  @Override
  public void visitUnaryLogicalOp(String logicalOp, ASTCriteriaVisitor visitedSubTree) {
    node.put(ESDriverConfig.LOGICAL_OPS.get(logicalOp), ((ESCriteriaVisitor) visitedSubTree).node);
  }

  @Override
  public void visitPredicate(String predicateOp, String leftColCanonical, List<String> rightExps)
    throws InvalidQueryException {
    final String leftCol = visitColumn(leftColCanonical);
    String elasticPredicateOp = ESDriverConfig.PREDICATES.get(predicateOp);
    if (elasticPredicateOp.equals(ESDriverConfig.TERM)) {
      final ObjectNode termNode = jsonNodeFactory.objectNode();
      termNode.put(leftCol, removeSingleQuotesFromLiteral(rightExps.get(0)));
      node.put(ESDriverConfig.TERM, termNode);
    } else if (elasticPredicateOp.equals(ESDriverConfig.TERMS)) {
      final ObjectNode termsNode = jsonNodeFactory.objectNode();
      final ArrayNode arrayNode = jsonNodeFactory.arrayNode();
      for (String right : rightExps) {
        arrayNode.add(removeSingleQuotesFromLiteral(right));
      }
      termsNode.put(leftCol, arrayNode);
      node.put(ESDriverConfig.TERMS, termsNode);
    } else if (ESDriverConfig.RANGE_PREDICATES.containsValue(elasticPredicateOp)) {
      final ObjectNode rangeNode = jsonNodeFactory.objectNode();
      final ObjectNode rangeInnerNode = jsonNodeFactory.objectNode();
      if (predicateOp.equals("between")) {
        Validate.isTrue(rightExps.size() == 2);
        rangeInnerNode.put("gt", removeSingleQuotesFromLiteral(rightExps.get(0)));
        rangeInnerNode.put("lt", removeSingleQuotesFromLiteral(rightExps.get(1)));
      } else {
        rangeInnerNode.put(elasticPredicateOp, removeSingleQuotesFromLiteral(rightExps.get(0)));
      }
      rangeNode.put(leftCol, rangeInnerNode);
      node.put(ESDriverConfig.RANGE, rangeNode);
    } else {
      throw new InvalidQueryException("No handlers for the registered predicate" + predicateOp);
    }
  }

  public ObjectNode getNode() {
    return node;
  }

  private static String visitColumn(String cannonicalColName) {
    final String[] colParts = cannonicalColName.split("\\.");
    return colParts[colParts.length - 1];
  }

  private static String removeSingleQuotesFromLiteral(String rightExp) {
    return rightExp.replaceAll("^\'|\'$", "");
  }
}
