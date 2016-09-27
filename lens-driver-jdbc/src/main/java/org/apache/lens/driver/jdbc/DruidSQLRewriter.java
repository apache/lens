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
package org.apache.lens.driver.jdbc;

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

import java.util.ArrayList;
import java.util.TreeSet;

import org.apache.lens.cube.parse.CubeSemanticAnalyzer;
import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DruidSQLRewriter extends ColumnarSQLRewriter {

  /**
   * Whether to resolve native tables or not. In case the query has sub query, the outer query may not
   * require native table resolution
   */
  private boolean resolveNativeTables;

  /**
   * Analyze internal.
   *
   * @throws SemanticException the semantic exception
   */
  public void analyzeInternal(Configuration conf, HiveConf hconf) throws SemanticException {
    CubeSemanticAnalyzer c1 = new CubeSemanticAnalyzer(conf, hconf);

    QB qb = new QB(null, null, false);

    if (!c1.doPhase1(ast, qb, c1.initPhase1Ctx(), null)) {
      return;
    }

    if (!qb.getSubqAliases().isEmpty()) {
      log.warn("Subqueries in from clause is not supported by {} Query : {}", this, this.query);
      throw new SemanticException("Subqueries in from clause is not supported by " + this + " Query : " + this.query);
    }

    // Get clause name
    TreeSet<String> ks = new TreeSet<String>(qb.getParseInfo().getClauseNames());
    /* The clause name. */
    String clauseName = ks.first();

    if (qb.getParseInfo().getJoinExpr() != null) {
      log.warn("Join queries not supported by {} Query : {}", this, this.query);
      throw new SemanticException("Join queries not supported by " + this + " Query : " + this.query);
    }
    // Split query into trees
    if (qb.getParseInfo().getWhrForClause(clauseName) != null) {
      this.whereAST = qb.getParseInfo().getWhrForClause(clauseName);
    }

    if (qb.getParseInfo().getHavingForClause(clauseName) != null) {
      this.havingAST = qb.getParseInfo().getHavingForClause(clauseName);
    }

    if (qb.getParseInfo().getOrderByForClause(clauseName) != null) {
      this.orderByAST = qb.getParseInfo().getOrderByForClause(clauseName);
    }

    if (qb.getParseInfo().getGroupByForClause(clauseName) != null) {
      this.groupByAST = qb.getParseInfo().getGroupByForClause(clauseName);
    }

    if (qb.getParseInfo().getSelForClause(clauseName) != null) {
      this.selectAST = qb.getParseInfo().getSelForClause(clauseName);
    }

    this.fromAST = HQLParser.findNodeByPath(ast, TOK_FROM);

  }

  /**
   * Builds the query.
   *
   * @throws SemanticException
   */
  public void buildDruidQuery(Configuration conf, HiveConf hconf) throws SemanticException, LensException {
    analyzeInternal(conf, hconf);
    if (resolveNativeTables) {
      replaceWithUnderlyingStorage(hconf);
    }

    // Get the limit clause
    String limit = getLimitClause(ast);

    ArrayList<String> filters = new ArrayList<>();
    getWhereString(whereAST, filters);

    // construct query with fact sub query
    constructQuery(HQLParser.getString(selectAST, HQLParser.AppendMode.DEFAULT), filters,
      HQLParser.getString(groupByAST, HQLParser.AppendMode.DEFAULT),
      HQLParser.getString(havingAST, HQLParser.AppendMode.DEFAULT),
      HQLParser.getString(orderByAST, HQLParser.AppendMode.DEFAULT), limit);

  }

  private ArrayList<String> getWhereString(ASTNode node, ArrayList<String> filters) throws LensException {

    if (node == null) {
      return null;
    }
    if (node.getToken().getType() == HiveParser.KW_AND) {
      // left child is "and" and right child is subquery
      if (node.getChild(0).getType() == HiveParser.KW_AND) {
        filters.add(getfilterSubquery(node, 1));
      } else if (node.getChildCount() > 1) {
        for (int i = 0; i < node.getChildCount(); i++) {
          filters.add(getfilterSubquery(node, i));
        }
      }
    } else if (node.getParent().getType() == HiveParser.TOK_WHERE
      && node.getToken().getType() != HiveParser.KW_AND) {
      filters.add(HQLParser.getString(node, HQLParser.AppendMode.DEFAULT));
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      return getWhereString(child, filters);
    }
    return filters;
  }

  private String getfilterSubquery(ASTNode node, int index) throws LensException {
    String filter;
    if (node.getChild(index).getType() == HiveParser.TOK_SUBQUERY_EXPR) {
      log.warn("Subqueries in where clause not supported by {} Query : {}", this, this.query);
      throw new LensException("Subqueries in where clause not supported by " + this + " Query : " + this.query);
    } else {
      filter = HQLParser.getString((ASTNode) node.getChild(index), HQLParser.AppendMode.DEFAULT);
    }
    return filter;
  }

  /**
   * Construct final query using all trees
   *
   * @param selecttree   the selecttree
   * @param whereFilters the wheretree
   * @param groupbytree  the groupbytree
   * @param havingtree   the havingtree
   * @param orderbytree  the orderbytree
   * @param limit        the limit
   */
  private void constructQuery(
    String selecttree, ArrayList<String> whereFilters, String groupbytree,
    String havingtree, String orderbytree, String limit) {

    log.info("In construct query ..");

    rewrittenQuery.append("select ").append(selecttree.replaceAll("`", "\"")).append(" from ");

    String factNameAndAlias = getFactNameAlias(fromAST);

    rewrittenQuery.append(factNameAndAlias);

    if (!whereFilters.isEmpty()) {
      rewrittenQuery.append(" where ").append(StringUtils.join(whereFilters, " and "));
    }
    if (StringUtils.isNotBlank(groupbytree)) {
      rewrittenQuery.append(" group by ").append(groupbytree);
    }
    if (StringUtils.isNotBlank(havingtree)) {
      rewrittenQuery.append(" having ").append(havingtree);
    }
    if (StringUtils.isNotBlank(orderbytree)) {
      rewrittenQuery.append(" order by ").append(orderbytree);
    }
    if (StringUtils.isNotBlank(limit)) {
      rewrittenQuery.append(" limit ").append(limit);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.query.QueryRewriter#rewrite(java.lang.String, org.apache.hadoop.conf.Configuration)
   */
  @Override
  public String rewrite(String query, Configuration conf, HiveConf metastoreConf) throws LensException {
    this.query = query;
    String reWritten = rewrite(HQLParser.parseHQL(query, metastoreConf), conf, metastoreConf, true);

    log.info("Rewritten : {}", reWritten);
    String queryReplacedUdf = replaceUDFForDB(reWritten);
    log.info("Input Query : {}", query);
    log.info("Rewritten Query : {}", queryReplacedUdf);
    return queryReplacedUdf;
  }

  public String rewrite(ASTNode currNode, Configuration conf, HiveConf metastoreConf, boolean resolveNativeTables)
    throws LensException {
    this.resolveNativeTables = resolveNativeTables;
    rewrittenQuery.setLength(0);
    reset();
    this.ast = currNode;

    ASTNode fromNode = HQLParser.findNodeByPath(currNode, TOK_FROM);
    if (fromNode != null) {
      if (fromNode.getChild(0).getType() == TOK_SUBQUERY) {
        log.warn("Subqueries in from clause not supported by {} Query : {}", this, this.query);
        throw new LensException("Subqueries in from clause not supported by " + this + " Query : " + this.query);
      } else if (isOfTypeJoin(fromNode.getChild(0).getType())) {
        log.warn("Join in from clause not supported by {} Query : {}", this, this.query);
        throw new LensException("Join in from clause not supported by " + this + " Query : " + this.query);
      }
    }

    if (currNode.getToken().getType() == TOK_UNIONALL) {
      log.warn("Union queries are not supported by {} Query : {}", this, this.query);
      throw new LensException("Union queries are not supported by " + this + " Query : " + this.query);
    }

    String rewritternQueryText = rewrittenQuery.toString();
    if (currNode.getToken().getType() == TOK_QUERY) {
      try {
        buildDruidQuery(conf, metastoreConf);
        rewritternQueryText = rewrittenQuery.toString();
        log.info("Rewritten query from build : " + rewritternQueryText);
      } catch (SemanticException e) {
        throw new LensException(e);
      }
    }
    return rewritternQueryText;
  }

  private boolean isOfTypeJoin(int type) {
    return (type == TOK_JOIN || type == TOK_LEFTOUTERJOIN || type == TOK_RIGHTOUTERJOIN
      || type == TOK_FULLOUTERJOIN || type == TOK_LEFTSEMIJOIN || type == TOK_UNIQUEJOIN);
  }

}
