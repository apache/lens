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

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

import java.util.*;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.*;
import org.apache.lens.cube.metadata.join.JoinPath;
import org.apache.lens.cube.parse.join.AutoJoinContext;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.*;

import com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;

/**
 * JoinResolver.
 */
@Slf4j
class JoinResolver implements ContextRewriter {
  private Map<AbstractCubeTable, JoinType> tableJoinTypeMap;
  private AbstractCubeTable target;
  private HashMap<Dimension, List<JoinChain>> dimensionInJoinChain = new HashMap<Dimension, List<JoinChain>>();

  public JoinResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
    tableJoinTypeMap = new HashMap<AbstractCubeTable, JoinType>();
    try {
      resolveJoins(cubeql);
    } catch (HiveException e) {
      throw new LensException(e);
    }
  }

  private void resolveJoins(CubeQueryContext cubeql) throws LensException, HiveException {
    QB cubeQB = cubeql.getQb();
    boolean joinResolverDisabled = cubeql.getConf().getBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS,
        CubeQueryConfUtil.DEFAULT_DISABLE_AUTO_JOINS);

    if (!joinResolverDisabled && (!cubeql.getNonChainedDimensions().isEmpty() && cubeql.hasCubeInQuery())
      || ((cubeql.getNonChainedDimensions().size() > 1) && !cubeql.hasCubeInQuery())) {
      log.warn("Disabling auto join resolver as there are direct dimensions queried");
      joinResolverDisabled = true;
    }
    if (joinResolverDisabled) {
      if (cubeql.getJoinAST() != null) {
        cubeQB.setQbJoinTree(genJoinTree(cubeql.getJoinAST(), cubeql));
      } else {
        if (cubeql.hasCubeInQuery()) {
          if (!cubeql.getNonChainedDimensions().isEmpty()) {
            throw new LensException(LensCubeErrorCode.NO_JOIN_CONDITION_AVAILABLE.getLensErrorInfo());
          }
        } else {
          if (cubeql.getNonChainedDimensions().size() > 1) {
            throw new LensException(LensCubeErrorCode.NO_JOIN_CONDITION_AVAILABLE.getLensErrorInfo());
          }
        }
      }
    } else {
      autoResolveJoins(cubeql);
    }
  }

  private void processJoinChains(CubeQueryContext cubeql) throws HiveException {
    for (JoinChain chain : cubeql.getJoinchains().values()) {
      Set<String> dims = chain.getIntermediateDimensions();

      dims.add(chain.getDestTable());
      for (String dim : dims) {
        Dimension dimension = cubeql.getMetastoreClient().getDimension(dim);
        if (dimensionInJoinChain.get(dimension) == null) {
          dimensionInJoinChain.put(dimension, new ArrayList<JoinChain>());
        }
        dimensionInJoinChain.get(dimension).add(chain);
      }
    }
  }

  /**
   * Resolve joins automatically for the given query.
   *
   * @param cubeql
   * @throws LensException
   * @throws HiveException
   */
  private void autoResolveJoins(CubeQueryContext cubeql) throws LensException, HiveException {
    if (cubeql.getJoinchains().isEmpty()) {
      // Joins not required
      log.info("No dimension tables to resolve and no join chains present!");
      return;
    }
    processJoinChains(cubeql);
    // Find the target
    if (cubeql.hasCubeInQuery()) {
      // Only cube in the query
      target = (AbstractCubeTable) cubeql.getCube();
    } else {
      String targetDimAlias = cubeql.getQb().getTabAliases().iterator().next();
      String targetDimTable = cubeql.getQb().getTabNameForAlias(targetDimAlias);
      if (targetDimTable == null) {
        log.warn("Null table for alias {}", targetDimAlias);
        throw new LensException(LensCubeErrorCode.JOIN_TARGET_NOT_CUBE_TABLE.getLensErrorInfo(), targetDimAlias);
      }
      target = cubeql.getMetastoreClient().getDimension(targetDimTable);
      if (target == null) {
        log.warn("Can't resolve joins for null target");
        throw new LensException(LensCubeErrorCode.JOIN_TARGET_NOT_CUBE_TABLE.getLensErrorInfo(), targetDimTable);
      }
    }

    for (JoinChain chain : cubeql.getJoinchains().values()) {
      for (String dimName : chain.getIntermediateDimensions()) {
        cubeql.addOptionalJoinDimTable(dimName, true);
      }
    }

    Map<Aliased<Dimension>, List<JoinPath>> multipleJoinPaths = new LinkedHashMap<>();

    // populate paths from joinchains
    for (JoinChain chain : cubeql.getJoinchains().values()) {
      Dimension dimension = cubeql.getMetastoreClient().getDimension(chain.getDestTable());
      Aliased<Dimension> aliasedDimension = Aliased.create(dimension, chain.getName());
      if (multipleJoinPaths.get(aliasedDimension) == null) {
        multipleJoinPaths.put(aliasedDimension, new ArrayList<JoinPath>());
      }
      multipleJoinPaths.get(aliasedDimension).addAll(
        chain.getRelationEdges(cubeql.getMetastoreClient()));
    }
    boolean flattenBridgeTables = cubeql.getConf().getBoolean(CubeQueryConfUtil.ENABLE_FLATTENING_FOR_BRIDGETABLES,
      CubeQueryConfUtil.DEFAULT_ENABLE_FLATTENING_FOR_BRIDGETABLES);
    String bridgeTableFieldAggr = cubeql.getConf().get(CubeQueryConfUtil.BRIDGE_TABLE_FIELD_AGGREGATOR,
      CubeQueryConfUtil.DEFAULT_BRIDGE_TABLE_FIELD_AGGREGATOR);
    Set<Dimension> requiredDimensions = Sets.newHashSet(cubeql.getDimensions());
    requiredDimensions.removeAll(cubeql.getOptionalDimensions());
    AutoJoinContext joinCtx =
      new AutoJoinContext(multipleJoinPaths, requiredDimensions,
        tableJoinTypeMap, target, cubeql.getConf().get(CubeQueryConfUtil.JOIN_TYPE_KEY), true, flattenBridgeTables,
        bridgeTableFieldAggr);
    cubeql.setAutoJoinCtx(joinCtx);
  }

  // Recursively find out join conditions
  private QBJoinTree genJoinTree(ASTNode joinParseTree, CubeQueryContext cubeql) throws LensException {
    QBJoinTree joinTree = new QBJoinTree();
    JoinCond[] condn = new JoinCond[1];

    // Figure out join condition descriptor
    switch (joinParseTree.getToken().getType()) {
    case TOK_LEFTOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.LEFTOUTER);
      break;
    case TOK_RIGHTOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.RIGHTOUTER);
      break;
    case TOK_FULLOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.FULLOUTER);
      break;
    case TOK_LEFTSEMIJOIN:
      joinTree.setNoSemiJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.LEFTSEMI);
      break;
    default:
      condn[0] = new JoinCond(0, 1, JoinType.INNER);
      joinTree.setNoOuterJoin(true);
      break;
    }

    joinTree.setJoinCond(condn);

    ASTNode left = (ASTNode) joinParseTree.getChild(0);
    ASTNode right = (ASTNode) joinParseTree.getChild(1);

    // Left subtree is table or a subquery
    if ((left.getToken().getType() == TOK_TABREF) || (left.getToken().getType() == TOK_SUBQUERY)) {
      String tableName = SemanticAnalyzer.getUnescapedUnqualifiedTableName((ASTNode) left.getChild(0)).toLowerCase();
      String alias =
        left.getChildCount() == 1 ? tableName : SemanticAnalyzer.unescapeIdentifier(left
          .getChild(left.getChildCount() - 1).getText().toLowerCase());

      joinTree.setLeftAlias(alias);

      String[] leftAliases = new String[1];
      leftAliases[0] = alias;
      joinTree.setLeftAliases(leftAliases);

      String[] children = new String[2];
      children[0] = alias;
      joinTree.setBaseSrc(children);

    } else if (isJoinToken(left)) {
      // Left subtree is join token itself, so recurse down
      QBJoinTree leftTree = genJoinTree(left, cubeql);

      joinTree.setJoinSrc(leftTree);

      String[] leftChildAliases = leftTree.getLeftAliases();
      String[] leftAliases = new String[leftChildAliases.length + 1];
      for (int i = 0; i < leftChildAliases.length; i++) {
        leftAliases[i] = leftChildAliases[i];
      }
      leftAliases[leftChildAliases.length] = leftTree.getRightAliases()[0];
      joinTree.setLeftAliases(leftAliases);

    } else {
      assert (false);
    }

    if ((right.getToken().getType() == TOK_TABREF) || (right.getToken().getType() == TOK_SUBQUERY)) {
      String tableName = SemanticAnalyzer.getUnescapedUnqualifiedTableName((ASTNode) right.getChild(0)).toLowerCase();
      String alias =
        right.getChildCount() == 1 ? tableName : SemanticAnalyzer.unescapeIdentifier(right
          .getChild(right.getChildCount() - 1).getText().toLowerCase());
      String[] rightAliases = new String[1];
      rightAliases[0] = alias;
      joinTree.setRightAliases(rightAliases);
      String[] children = joinTree.getBaseSrc();
      if (children == null) {
        children = new String[2];
      }
      children[1] = alias;
      joinTree.setBaseSrc(children);
      // remember rhs table for semi join
      if (!joinTree.getNoSemiJoin()) {
        joinTree.addRHSSemijoin(alias);
      }
    } else {
      assert false;
    }

    ASTNode joinCond = (ASTNode) joinParseTree.getChild(2);
    if (joinCond != null) {
      cubeql.setJoinCond(joinTree, HQLParser.getString(joinCond));
    } else {
      // No join condition specified. this should be an error
      throw new LensException(LensCubeErrorCode.NO_JOIN_CONDITION_AVAILABLE.getLensErrorInfo());
    }
    return joinTree;
  }

  private static boolean isJoinToken(ASTNode node) {
    return (node.getToken().getType() == TOK_JOIN) || (node.getToken().getType() == TOK_LEFTOUTERJOIN)
      || (node.getToken().getType() == TOK_RIGHTOUTERJOIN) || (node.getToken().getType() == TOK_FULLOUTERJOIN)
      || (node.getToken().getType() == TOK_LEFTSEMIJOIN) || (node.getToken().getType() == TOK_UNIQUEJOIN);
  }
}
