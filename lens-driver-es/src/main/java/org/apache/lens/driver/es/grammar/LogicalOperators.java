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

import org.apache.lens.driver.es.exceptions.InvalidQueryException;

import org.apache.commons.lang3.Validate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;

public enum LogicalOperators {
  and {
    @Override
    public void build(ObjectNode targetNode, List<JsonNode> innerNodes) {
      final ArrayNode subTrees = JSON_NODE_FACTORY.arrayNode();
      subTrees.addAll(innerNodes);
      targetNode.put("and", subTrees);
    }
  },
  or {
    @Override
    public void build(ObjectNode targetNode, List<JsonNode> innerNodes) {
      final ArrayNode subTrees = JSON_NODE_FACTORY.arrayNode();
      subTrees.addAll(innerNodes);
      targetNode.put("or", subTrees);
    }
  },
  not {
    @Override
    public void build(ObjectNode targetNode, List<JsonNode> innerNodes) {
      Validate.isTrue(innerNodes.size() == 1, "Not can have exactly only one child");
      targetNode.put("not", innerNodes.get(0));
    }
  };

  protected static final JsonNodeFactory JSON_NODE_FACTORY = JsonNodeFactory.instance;

  public abstract void build(ObjectNode targetNode, List<JsonNode> innerNodes);

  private static final ImmutableMap<String, LogicalOperators> HQL_LOG_OP_MAP;
  static {
    final ImmutableMap.Builder<String, LogicalOperators> logicalOpsBuilder = ImmutableMap.builder();
    logicalOpsBuilder.put("and", and);
    logicalOpsBuilder.put("&&", and);
    logicalOpsBuilder.put("&", and);
    logicalOpsBuilder.put("or", or);
    logicalOpsBuilder.put("||", or);
    logicalOpsBuilder.put("|", or);
    logicalOpsBuilder.put("!", not);
    logicalOpsBuilder.put("not", not);
    HQL_LOG_OP_MAP = logicalOpsBuilder.build();
  }

  public static LogicalOperators getFor(String hqlLop) throws InvalidQueryException {
    if (HQL_LOG_OP_MAP.containsKey(hqlLop)) {
      return HQL_LOG_OP_MAP.get(hqlLop);
    }
    throw new InvalidQueryException("Handler not available for logical operator " + hqlLop);
  }
}
