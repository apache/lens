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

import org.apache.lens.driver.es.exceptions.InvalidQueryException;
import org.apache.lens.driver.es.grammar.LogicalOperators;
import org.apache.lens.driver.es.grammar.Predicates;
import org.apache.lens.driver.es.translator.ASTCriteriaVisitor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;


public final class ESCriteriaVisitor implements ASTCriteriaVisitor {

  private final JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;

  private ObjectNode node = jsonNodeFactory.objectNode();

  @Override
  public void visitLogicalOp(String logicalOp, List<ASTCriteriaVisitor> visitedSubTrees) throws InvalidQueryException {
    LogicalOperators.getFor(logicalOp)
      .build(node, collectNodesFromVisitors(visitedSubTrees));
  }

  @Override
  public void visitPredicate(String predicateOp, String leftColCanonical, List<String> rightExps)
    throws InvalidQueryException {
    final String leftCol = visitColumn(leftColCanonical);
    Predicates.getFor(predicateOp)
      .build(node, leftCol, rightExps);
  }

  public ObjectNode getNode() {
    return node;
  }

  private static List<JsonNode> collectNodesFromVisitors(List<ASTCriteriaVisitor> visitedSubTrees) {
    final List<JsonNode> subTrees = Lists.newArrayList();
    for(ASTCriteriaVisitor visitor: visitedSubTrees) {
      subTrees.add(((ESCriteriaVisitor)visitor).node);
    }
    return subTrees;
  }

  private static String visitColumn(String cannonicalColName) {
    final String[] colParts = cannonicalColName.split("\\.");
    return colParts[colParts.length - 1];
  }

}
