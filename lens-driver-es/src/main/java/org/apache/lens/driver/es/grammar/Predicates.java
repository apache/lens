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

import java.util.List;

import org.apache.lens.driver.es.ESDriverConfig;
import org.apache.lens.driver.es.exceptions.InvalidQueryException;

import org.apache.commons.lang.Validate;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;

public enum Predicates {
  term {
    @Override
    public void build(ObjectNode targetNode, String leftCol, List<String> rightExps) {
      final ObjectNode termNode = JSON_NODE_FACTORY.objectNode();
      termNode.put(leftCol, removeSingleQuotesFromLiteral(rightExps.get(0)));
      targetNode.put(ESDriverConfig.TERM, termNode);
    }
  },
  not_term {
    @Override
    public void build(ObjectNode targetNode, String leftCol, List<String> rightExps) {
      final ObjectNode innerNode = addNotNode(targetNode);
      term.build(innerNode, leftCol, rightExps);
    }
  },
  terms {
    @Override
    public void build(ObjectNode targetNode, String leftCol, List<String> rightExps) {
      final ObjectNode termsNode = JSON_NODE_FACTORY.objectNode();
      final ArrayNode arrayNode = JSON_NODE_FACTORY.arrayNode();
      for (String right : rightExps) {
        arrayNode.add(removeSingleQuotesFromLiteral(right));
      }
      termsNode.put(leftCol, arrayNode);
      targetNode.put(ESDriverConfig.TERMS, termsNode);
    }
  },
  not_terms {
    @Override
    public void build(ObjectNode targetNode, String leftCol, List<String> rightExps) {
      final ObjectNode innerNode = addNotNode(targetNode);
      terms.build(innerNode, leftCol, rightExps);
    }
  },
  gt {
    @Override
    public void build(ObjectNode targetNode, String leftCol, List<String> rightExps) {
      final ObjectNode rangeInnerNode = getRangeNode(targetNode, leftCol, rightExps);
      rangeInnerNode.put("gt", removeSingleQuotesFromLiteral(rightExps.get(0)));
    }
  },
  gte {
    @Override
    public void build(ObjectNode targetNode, String leftCol, List<String> rightExps) {
      final ObjectNode rangeInnerNode = getRangeNode(targetNode, leftCol, rightExps);
      rangeInnerNode.put("gte", removeSingleQuotesFromLiteral(rightExps.get(0)));
    }
  },
  lt {
    @Override
    public void build(ObjectNode targetNode, String leftCol, List<String> rightExps) {
      final ObjectNode rangeInnerNode = getRangeNode(targetNode, leftCol, rightExps);
      rangeInnerNode.put("lt", removeSingleQuotesFromLiteral(rightExps.get(0)));
    }
  },
  lte {
    @Override
    public void build(ObjectNode targetNode, String leftCol, List<String> rightExps) {
      final ObjectNode rangeInnerNode = getRangeNode(targetNode, leftCol, rightExps);
      rangeInnerNode.put("lte", removeSingleQuotesFromLiteral(rightExps.get(0)));
    }
  },
  range {
    @Override
    public void build(ObjectNode targetNode, String leftCol, List<String> rightExps) {
      Validate.isTrue(rightExps.size() == 2);
      final ObjectNode rangeInnerNode = getRangeNode(targetNode, leftCol, rightExps);
      rangeInnerNode.put("gt", removeSingleQuotesFromLiteral(rightExps.get(0)));
      rangeInnerNode.put("lt", removeSingleQuotesFromLiteral(rightExps.get(1)));
    }
  };

  private static ObjectNode getRangeNode(ObjectNode targetNode, String leftCol, List<String> rightExps) {
    final ObjectNode rangeNode = JSON_NODE_FACTORY.objectNode();
    final ObjectNode rangeInnerNode = JSON_NODE_FACTORY.objectNode();
    rangeNode.put(leftCol, rangeInnerNode);
    targetNode.put(ESDriverConfig.RANGE, rangeNode);
    return rangeInnerNode;
  }

  protected static final JsonNodeFactory JSON_NODE_FACTORY = JsonNodeFactory.instance;

  private static String removeSingleQuotesFromLiteral(String rightExp) {
    return rightExp.replaceAll("^\'|\'$", "");
  }

  protected final ObjectNode addNotNode(ObjectNode targetNode) {
    final ObjectNode innerPredicateNode = JSON_NODE_FACTORY.objectNode();
    targetNode.put("not", innerPredicateNode);
    return innerPredicateNode;
  }

  public abstract void build(ObjectNode targetNode, String leftCol, List<String> rightExps);

  public static Predicates getFor(String hqlPredicate) throws InvalidQueryException {
    if (HQL_PREDICATE_MAP.containsKey(hqlPredicate)) {
      return HQL_PREDICATE_MAP.get(hqlPredicate);
    }
    throw new InvalidQueryException("Cannot find a handler for the hql predicate " + hqlPredicate);
  }

  private static final ImmutableMap<String, Predicates> HQL_PREDICATE_MAP;
  static {
    final ImmutableMap.Builder<String, Predicates> predicatesBuilder = ImmutableMap.builder();
    predicatesBuilder.put(">", gt);
    predicatesBuilder.put(">=", gte);
    predicatesBuilder.put("<", lt);
    predicatesBuilder.put("<=", lte);
    predicatesBuilder.put("between", range);
    predicatesBuilder.put("=", term);
    predicatesBuilder.put("!=", not_term);
    predicatesBuilder.put("in", terms);
    predicatesBuilder.put("not in", not_terms);
    HQL_PREDICATE_MAP = predicatesBuilder.build();
  }

}
