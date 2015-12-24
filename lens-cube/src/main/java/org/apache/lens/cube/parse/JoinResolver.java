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
import org.apache.lens.cube.metadata.SchemaGraph.TableRelationship;
import org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.*;

import lombok.extern.slf4j.Slf4j;

/**
 * JoinResolver.
 */
@Slf4j
class JoinResolver implements ContextRewriter {

  private Map<AbstractCubeTable, String> partialJoinConditions;
  private Map<AbstractCubeTable, JoinType> tableJoinTypeMap;
  private boolean partialJoinChain;
  private AbstractCubeTable target;
  private HashMap<Dimension, List<JoinChain>> dimensionInJoinChain = new HashMap<Dimension, List<JoinChain>>();

  public JoinResolver(Configuration conf) {
  }

  static String getJoinTypeStr(JoinType joinType) {
    if (joinType == null) {
      return "";
    }
    switch (joinType) {
    case FULLOUTER:
      return " full outer";
    case INNER:
      return " inner";
    case LEFTOUTER:
      return " left outer";
    case LEFTSEMI:
      return " left semi";
    case UNIQUE:
      return " unique";
    case RIGHTOUTER:
      return " right outer";
    default:
      return "";
    }
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
    partialJoinConditions = new HashMap<AbstractCubeTable, String>();
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
    if (joinResolverDisabled) {
      if (cubeql.getJoinAST() != null) {
        cubeQB.setQbJoinTree(genJoinTree(cubeql.getJoinAST(), cubeql));
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
    // Check if this query needs a join -
    // A join is needed if there is a cube and at least one dimension, or, 0
    // cubes and more than one
    // dimensions
    processJoinChains(cubeql);
    Set<Dimension> dimensions = cubeql.getNonChainedDimensions();
    // Add dimensions specified in the partial join tree
    ASTNode joinClause = cubeql.getQb().getParseInfo().getJoinExpr();
    if (joinClause == null) {
      // Only cube in the query
      if (cubeql.hasCubeInQuery()) {
        target = (AbstractCubeTable) cubeql.getCube();
      } else {
        String targetDimAlias = cubeql.getQb().getTabAliases().iterator().next();
        String targetDimTable = cubeql.getQb().getTabNameForAlias(targetDimAlias);
        if (targetDimTable == null) {
          log.warn("Null table for alias {}", targetDimAlias);
          return;
        }
        target = cubeql.getMetastoreClient().getDimension(targetDimTable);
      }
    }
    searchDimensionTables(cubeql.getMetastoreClient(), joinClause);
    if (target == null) {
      log.warn("Can't resolve joins for null target");
      return;
    }

    Set<Dimension> dimTables = new HashSet<Dimension>(dimensions);
    for (AbstractCubeTable partiallyJoinedTable : partialJoinConditions.keySet()) {
      dimTables.add((Dimension) partiallyJoinedTable);
    }

    for (JoinChain chain : cubeql.getJoinchains().values()) {
      for (String dimName : chain.getIntermediateDimensions()) {
        cubeql.addOptionalJoinDimTable(dimName, true);
      }
    }

    // Remove target
    dimTables.remove(target);
    if (dimTables.isEmpty() && cubeql.getJoinchains().isEmpty()) {
      // Joins not required
      log.info("No dimension tables to resolve and no join chains present!");
      return;
    }


    SchemaGraph graph = cubeql.getMetastoreClient().getSchemaGraph();
    Map<Aliased<Dimension>, List<SchemaGraph.JoinPath>> multipleJoinPaths =
      new LinkedHashMap<Aliased<Dimension>, List<SchemaGraph.JoinPath>>();

    // Resolve join path for each dimension accessed in the query
    for (Dimension joinee : dimTables) {
      if (dimensionInJoinChain.get(joinee) == null) {
        // Find all possible join paths
        SchemaGraph.GraphSearch search = new SchemaGraph.GraphSearch(joinee, target, graph);
        List<SchemaGraph.JoinPath> joinPaths = search.findAllPathsToTarget();
        if (joinPaths != null && !joinPaths.isEmpty()) {
          Aliased<Dimension> aliasedJoinee = Aliased.create(joinee);
          multipleJoinPaths.put(aliasedJoinee, new ArrayList<SchemaGraph.JoinPath>(search.findAllPathsToTarget()));
          addOptionalTables(cubeql, multipleJoinPaths.get(aliasedJoinee), cubeql.getDimensions().contains(joinee));
        } else {
          // No link to cube from this dim, can't proceed with query
          if (log.isDebugEnabled()) {
            graph.print();
          }
          log.warn("No join path between {} and {}", joinee.getName(), target.getName());
          if (cubeql.getDimensions().contains(joinee)) {
            throw new LensException(LensCubeErrorCode.NO_JOIN_PATH.getLensErrorInfo(),
                joinee.getName(), target.getName());
          } else {
            // if joinee is optional dim table, remove those candidate facts
            Set<CandidateTable> candidates = cubeql.getOptionalDimensionMap().get(joinee).requiredForCandidates;
            for (CandidateTable candidate : candidates) {
              if (candidate instanceof CandidateFact) {
                if (cubeql.getCandidateFacts().contains(candidate)) {
                  log.info("Not considering fact:{} as there is no join path to {}", candidate, joinee);
                  cubeql.getCandidateFacts().remove(candidate);
                  cubeql.addFactPruningMsgs(((CandidateFact) candidate).fact, new CandidateTablePruneCause(
                    CandidateTablePruneCode.COLUMN_NOT_FOUND));
                }
              } else if (cubeql.getCandidateDimTables().containsKey(((CandidateDim) candidate).getBaseTable())) {
                log.info("Not considering dimtable:{} as there is no join path to {}", candidate, joinee);
                cubeql.getCandidateDimTables().get(((CandidateDim) candidate).getBaseTable()).remove(candidate);
                cubeql.addDimPruningMsgs(
                  (Dimension) candidate.getBaseTable(), (CubeDimensionTable) candidate.getTable(),
                  new CandidateTablePruneCause(CandidateTablePruneCode.COLUMN_NOT_FOUND)
                );
              }
            }
          }
        }
      } else if (dimensionInJoinChain.get(joinee).size() > 1) {
        throw new LensException("Table " + joinee.getName() + " has "
          +dimensionInJoinChain.get(joinee).size() + " different paths through joinchains "
          +"(" + dimensionInJoinChain.get(joinee) + ")"
          +" used in query. Couldn't determine which one to use");
      } else {
        // the case when dimension is used only once in all joinchains.
        if (isJoinchainDestination(cubeql, joinee)) {
          throw new LensException("Table " + joinee.getName() + " is getting accessed via two different names: "
            + "[" + dimensionInJoinChain.get(joinee).get(0).getName() + ", " + joinee.getName() + "]");
        }
        // table is accessed with chain and no chain
        if (cubeql.getNonChainedDimensions().contains(joinee)) {
          throw new LensException("Table " + joinee.getName() + " is getting accessed via joinchain: "
            + dimensionInJoinChain.get(joinee).get(0).getName() + " and no chain at all");
        }
      }
    }
    // populate paths from joinchains
    for (JoinChain chain : cubeql.getJoinchains().values()) {
      Dimension dimension = cubeql.getMetastoreClient().getDimension(chain.getDestTable());
      Aliased<Dimension> aliasedDimension = Aliased.create(dimension, chain.getName());
      if (multipleJoinPaths.get(aliasedDimension) == null) {
        multipleJoinPaths.put(aliasedDimension, new ArrayList<SchemaGraph.JoinPath>());
      }
      multipleJoinPaths.get(aliasedDimension).addAll(
        chain.getRelationEdges(cubeql.getMetastoreClient()));
    }
    boolean flattenBridgeTables = cubeql.getConf().getBoolean(CubeQueryConfUtil.ENABLE_FLATTENING_FOR_BRIDGETABLES,
      CubeQueryConfUtil.DEFAULT_ENABLE_FLATTENING_FOR_BRIDGETABLES);
    String bridgeTableFieldAggr = cubeql.getConf().get(CubeQueryConfUtil.BRIDGE_TABLE_FIELD_AGGREGATOR,
      CubeQueryConfUtil.DEFAULT_BRIDGE_TABLE_FIELD_AGGREGATOR);
    AutoJoinContext joinCtx =
      new AutoJoinContext(multipleJoinPaths, cubeql.optionalDimensions, partialJoinConditions, partialJoinChain,
        tableJoinTypeMap, target, cubeql.getConf().get(CubeQueryConfUtil.JOIN_TYPE_KEY), true, flattenBridgeTables,
        bridgeTableFieldAggr);
    cubeql.setAutoJoinCtx(joinCtx);
  }

  private boolean isJoinchainDestination(CubeQueryContext cubeql, Dimension dimension) {
    for (JoinChain chain : cubeql.getJoinchains().values()) {
      if (chain.getDestTable().equalsIgnoreCase(dimension.getName())) {
        return true;
      }
    }
    return false;
  }

  private void addOptionalTables(CubeQueryContext cubeql, List<SchemaGraph.JoinPath> joinPathList, boolean required)
    throws LensException {
    for (SchemaGraph.JoinPath joinPath : joinPathList) {
      for (TableRelationship rel : joinPath.getEdges()) {
        // Add the joined tables to the queries table sets so that they are
        // resolved in candidate resolver
        cubeql.addOptionalJoinDimTable(rel.getToTable().getName(), required);
      }
    }
  }

  private void setTarget(CubeMetastoreClient metastore, ASTNode node) throws  HiveException, LensException  {
    String targetTableName = HQLParser.getString(HQLParser.findNodeByPath(node, TOK_TABNAME, Identifier));
    if (metastore.isDimension(targetTableName)) {
      target = metastore.getDimension(targetTableName);
    } else if (metastore.isCube(targetTableName)) {
      target = (AbstractCubeTable) metastore.getCube(targetTableName);
    } else {
      throw new LensException(LensCubeErrorCode.JOIN_TARGET_NOT_CUBE_TABLE.getLensErrorInfo(), targetTableName);
    }
  }

  private void searchDimensionTables(CubeMetastoreClient metastore, ASTNode node) throws HiveException, LensException {
    if (node == null) {
      return;
    }
    // User has specified join conditions partially. We need to store join
    // conditions as well as join types
    partialJoinChain = true;
    if (isJoinToken(node)) {
      ASTNode left = (ASTNode) node.getChild(0);
      ASTNode right = (ASTNode) node.getChild(1);
      // Get table name and

      String tableName = HQLParser.getString(HQLParser.findNodeByPath(right, TOK_TABNAME, Identifier));

      Dimension dimension = metastore.getDimension(tableName);
      String joinCond = "";
      if (node.getChildCount() > 2) {
        // User has specified a join condition for filter pushdown.
        joinCond = HQLParser.getString((ASTNode) node.getChild(2));
      }
      partialJoinConditions.put(dimension, joinCond);
      tableJoinTypeMap.put(dimension, getJoinType(node));
      if (isJoinToken(left)) {
        searchDimensionTables(metastore, left);
      } else {
        if (left.getToken().getType() == TOK_TABREF) {
          setTarget(metastore, left);
        }
      }
    } else if (node.getToken().getType() == TOK_TABREF) {
      setTarget(metastore, node);
    }

  }

  private JoinType getJoinType(ASTNode node) {
    switch (node.getToken().getType()) {
    case TOK_LEFTOUTERJOIN:
      return JoinType.LEFTOUTER;
    case TOK_LEFTSEMIJOIN:
      return JoinType.LEFTSEMI;
    case TOK_RIGHTOUTERJOIN:
      return JoinType.RIGHTOUTER;
    case TOK_FULLOUTERJOIN:
      return JoinType.FULLOUTER;
    case TOK_JOIN:
      return JoinType.INNER;
    case TOK_UNIQUEJOIN:
      return JoinType.UNIQUE;
    default:
      return JoinType.INNER;
    }
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
      // remember rhs table for semijoin
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
      throw new LensException(LensCubeErrorCode.NO_JOIN_CONDITION_AVAIABLE.getLensErrorInfo());
    }
    return joinTree;
  }

  private static boolean isJoinToken(ASTNode node) {
    return (node.getToken().getType() == TOK_JOIN) || (node.getToken().getType() == TOK_LEFTOUTERJOIN)
      || (node.getToken().getType() == TOK_RIGHTOUTERJOIN) || (node.getToken().getType() == TOK_FULLOUTERJOIN)
      || (node.getToken().getType() == TOK_LEFTSEMIJOIN) || (node.getToken().getType() == TOK_UNIQUEJOIN);
  }
}
