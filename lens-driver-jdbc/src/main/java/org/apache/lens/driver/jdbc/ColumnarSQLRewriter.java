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
package org.apache.lens.driver.jdbc;

import java.util.*;
import org.antlr.runtime.CommonToken;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.lens.api.LensException;
import org.apache.lens.cube.metadata.CubeMetastoreClient;
import org.apache.lens.cube.parse.CubeSemanticAnalyzer;
import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.server.api.LensConfConstants;
import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

/**
 * The Class ColumnarSQLRewriter.
 */
public class ColumnarSQLRewriter implements QueryRewriter {

  /** The conf. */
  private Configuration conf;

  /** The clause name. */
  private String clauseName = null;

  /** The qb. */
  private QB qb;

  /** The ast. */
  protected ASTNode ast;

  /** The query. */
  protected String query;

  /** The final fact query. */
  private String finalFactQuery;

  /** The limit. */
  private String limit;

  /** The fact filters. */
  private StringBuilder factFilters = new StringBuilder();

  /** The fact in line query. */
  private StringBuilder factInLineQuery = new StringBuilder();

  /** The all sub queries. */
  protected StringBuilder allSubQueries = new StringBuilder();

  /** The fact keys. */
  protected StringBuilder factKeys = new StringBuilder();

  /** The rewritten query. */
  protected StringBuilder rewrittenQuery = new StringBuilder();

  /** The merged query. */
  protected StringBuilder mergedQuery = new StringBuilder();

  /** The join condition. */
  protected StringBuilder joinCondition = new StringBuilder();

  /** The allkeys. */
  protected List<String> allkeys = new ArrayList<String>();

  /** The agg column. */
  protected List<String> aggColumn = new ArrayList<String>();

  /** The filter in join cond. */
  protected List<String> filterInJoinCond = new ArrayList<String>();

  /** The right filter. */
  protected List<String> rightFilter = new ArrayList<String>();

  /** The left filter. */
  private String leftFilter;

  /** The map agg tab alias. */
  private Map<String, String> mapAggTabAlias = new HashMap<String, String>();

  /** The Constant LOG. */
  private static final Log LOG = LogFactory.getLog(ColumnarSQLRewriter.class);

  /** The where tree. */
  private String whereTree;

  /** The having tree. */
  private String havingTree;

  /** The order by tree. */
  private String orderByTree;

  /** The select tree. */
  private String selectTree;

  /** The group by tree. */
  private String groupByTree;

  /** The join tree. */
  private String joinTree;

  /** The from tree. */
  private String fromTree;

  /** The join ast. */
  private ASTNode joinAST;

  /** The having ast. */
  private ASTNode havingAST;

  /** The select ast. */
  private ASTNode selectAST;

  /** The where ast. */
  private ASTNode whereAST;

  /** The order by ast. */
  private ASTNode orderByAST;

  /** The group by ast. */
  private ASTNode groupByAST;

  /** The from ast. */
  protected ASTNode fromAST;

  /**
   * Instantiates a new columnar sql rewriter.
   *
   * @param conf
   *          the conf
   * @param query
   *          the query
   */
  public ColumnarSQLRewriter(HiveConf conf, String query) {
    // super(conf);
    this.conf = conf;
    this.query = query;
  }

  /**
   * Instantiates a new columnar sql rewriter.
   */
  public ColumnarSQLRewriter() {
    // super(conf);
  }

  public String getClause() {
    if (clauseName == null) {
      TreeSet<String> ks = new TreeSet<String>(qb.getParseInfo().getClauseNames());
      clauseName = ks.first();
    }
    return clauseName;
  }

  /*
   * Analyze query AST and split into trees
   */
  /**
   * Analyze internal.
   *
   * @throws SemanticException
   *           the semantic exception
   */
  public void analyzeInternal() throws SemanticException {
    HiveConf conf = new HiveConf();
    CubeSemanticAnalyzer c1 = new CubeSemanticAnalyzer(conf);

    QB qb = new QB(null, null, false);

    if (!c1.doPhase1(ast, qb, c1.initPhase1Ctx())) {
      return;
    }

    if (!qb.getSubqAliases().isEmpty()) {
      LOG.warn("Subqueries in from clause is not supported by " + this + " Query : " + this.query);
      throw new SemanticException("Subqueries in from clause is not supported by " + this + " Query : " + this.query);
    }

    // Get clause name
    TreeSet<String> ks = new TreeSet<String>(qb.getParseInfo().getClauseNames());
    clauseName = ks.first();

    // Split query into trees
    if (qb.getParseInfo().getWhrForClause(clauseName) != null) {
      this.whereTree = HQLParser.getString(qb.getParseInfo().getWhrForClause(clauseName));
      this.whereAST = qb.getParseInfo().getWhrForClause(clauseName);
    }

    if (qb.getParseInfo().getHavingForClause(clauseName) != null) {
      this.havingTree = HQLParser.getString(qb.getParseInfo().getHavingForClause(clauseName));
      this.havingAST = qb.getParseInfo().getHavingForClause(clauseName);
    }

    if (qb.getParseInfo().getOrderByForClause(clauseName) != null) {
      this.orderByTree = HQLParser.getString(qb.getParseInfo().getOrderByForClause(clauseName));
      this.orderByAST = qb.getParseInfo().getOrderByForClause(clauseName);
    }
    if (qb.getParseInfo().getGroupByForClause(clauseName) != null) {
      this.groupByTree = HQLParser.getString(qb.getParseInfo().getGroupByForClause(clauseName));
      this.groupByAST = qb.getParseInfo().getGroupByForClause(clauseName);
    }

    if (qb.getParseInfo().getSelForClause(clauseName) != null) {
      this.selectTree = HQLParser.getString(qb.getParseInfo().getSelForClause(clauseName));
      this.selectAST = qb.getParseInfo().getSelForClause(clauseName);
    }

    this.joinTree = HQLParser.getString(qb.getParseInfo().getJoinExpr());
    this.joinAST = qb.getParseInfo().getJoinExpr();

    this.fromAST = HQLParser.findNodeByPath(ast, TOK_FROM);
    this.fromTree = HQLParser.getString(fromAST);

  }

  /*
   * Get the table qualified name eg. database.table_name table_alias
   */

  /**
   * Gets the table from tab ref node.
   *
   * @param tree
   *          the tree
   * @return the table from tab ref node
   */
  public String getTableFromTabRefNode(ASTNode tree) {
    String table = "";
    ASTNode tabName = (ASTNode) tree.getChild(0);
    if (tabName.getChildCount() == 2) {
      table = tabName.getChild(0).getText() + "." + tabName.getChild(1).getText();
    } else {
      table = tabName.getChild(0).getText();
    }
    if (tree.getChildCount() > 1) {
      table = table + " " + tree.getChild(1).getText();
    }
    return table;
  }

  /*
   * Get join conditions specified in join clause
   */

  /**
   * Gets the join cond.
   *
   * @param node
   *          the node
   * @return the join cond
   */
  public void getJoinCond(ASTNode node) {
    if (node == null) {
      return;
    }
    int rootType = node.getToken().getType();
    String rightTable = "";

    if (rootType == TOK_JOIN || rootType == TOK_LEFTOUTERJOIN || rootType == TOK_RIGHTOUTERJOIN
        || rootType == TOK_FULLOUTERJOIN || rootType == TOK_LEFTSEMIJOIN || rootType == TOK_UNIQUEJOIN) {

      ASTNode left = (ASTNode) node.getChild(0);
      ASTNode right = (ASTNode) node.getChild(1);

      rightTable = getTableFromTabRefNode(right);
      String joinType = "";
      String joinFilter = "";
      String JoinToken = node.getToken().getText();

      if (JoinToken.equals("TOK_JOIN")) {
        joinType = "inner join";
      } else if (JoinToken.equals("TOK_LEFTOUTERJOIN")) {
        joinType = "left outer join";
      } else if (JoinToken.equals("TOK_RIGHTOUTERJOIN")) {
        joinType = "right outer join";
      } else if (JoinToken.equals("TOK_FULLOUTERJOIN")) {
        joinType = "full outer join";
      } else if (JoinToken.equals("TOK_LEFTSEMIJOIN")) {
        joinType = "left semi join";
      } else if (JoinToken.equals("TOK_UNIQUEJOIN")) {
        joinType = "unique join";
      } else {
        LOG.info("Non supported join type : " + JoinToken);
      }

      if (node.getChildCount() > 2) {
        // User has specified a join condition for filter pushdown.
        joinFilter = HQLParser.getString((ASTNode) node.getChild(2));
      }

      joinCondition.append(" ").append(joinType).append(" ").append(rightTable).append(" on ").append(joinFilter)
          .append(" ");

    }

    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      getJoinCond(child);
    }
  }

  /*
   * Get filter conditions if user has specified a join condition for filter pushdown.
   */

  /**
   * Gets the filter in join cond.
   *
   * @param node
   *          the node
   * @return the filter in join cond
   */
  public void getFilterInJoinCond(ASTNode node) {

    if (node == null) {
      LOG.debug("Join AST is null " + node);
      return;
    }

    if (node.getToken().getType() == HiveParser.KW_AND) {
      ASTNode right = (ASTNode) node.getChild(1);
      String filterCond = HQLParser.getString((ASTNode) right);
      rightFilter.add(filterCond);
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      getFilterInJoinCond(child);
    }
  }

  /*
   * Build fact sub query using where tree and join tree
   */

  /**
   * Builds the subqueries.
   *
   * @param node
   *          the node
   */
  public void buildSubqueries(ASTNode node) {
    if (node == null) {
      LOG.debug("Join AST is null " + node);
      return;
    }

    String subquery = "";
    if (node.getToken().getType() == HiveParser.EQUAL) {
      if (node.getChild(0).getType() == HiveParser.DOT && node.getChild(1).getType() == HiveParser.DOT) {

        ASTNode left = (ASTNode) node.getChild(0);
        ASTNode right = (ASTNode) node.getChild(1);

        // Get the fact and dimension columns in table_name.column_name format
        String factJoinKeys = HQLParser.getString((ASTNode) left).toString().replaceAll("\\s+", "")
            .replaceAll("[(,)]", "");
        String dimJoinKeys = HQLParser.getString((ASTNode) right).toString().replaceAll("\\s+", "")
            .replaceAll("[(,)]", "");

        String dimTableName = dimJoinKeys.substring(0, dimJoinKeys.indexOf("."));
        factKeys.append(factJoinKeys).append(",");

        // Construct part of subquery by referring join condition
        // fact.fact_key = dim_table.dim_key
        // eg. "fact_key in ( select dim_key from dim_table where "
        String queryphase1 = factJoinKeys.concat(" in ").concat(" ( ").concat(" select ").concat(dimJoinKeys)
            .concat(" from ").concat(dimTableName).concat(" where ");

        getAllFilters(whereAST);
        rightFilter.add(leftFilter);

        Set<String> setAllFilters = new HashSet<String>(rightFilter);

        // Check the occurrence of dimension table in the filter list and
        // combine all filters of same dimension table with and .
        // eg. "dim_table.key1 = 'abc' and dim_table.key2 = 'xyz'"
        if (setAllFilters.toString().matches("(.*)".toString().concat(dimTableName).concat("(.*)"))) {

          factFilters.delete(0, factFilters.length());

          // All filters in where clause
          for (int i = 0; i < setAllFilters.toArray().length; i++) {
            if (setAllFilters.toArray() != null) {
              if (setAllFilters.toArray()[i].toString().matches("(.*)".toString().concat(dimTableName).concat("(.*)"))) {
                String filters2 = setAllFilters.toArray()[i].toString().concat(" and ");
                factFilters.append(filters2);
              }
            }
          }
          // Merge fact subquery and dim subqury to construct the final subquery
          // eg. "fact_key in ( select dim_key from dim_table where
          // dim_table.key2 = 'abc' and dim_table.key3 = 'xyz'"
          subquery = queryphase1.concat(factFilters.toString().substring(0, factFilters.toString().lastIndexOf("and")))
              .concat(")");
          allSubQueries.append(subquery).append(" and ");
        }
      }
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      buildSubqueries(child);
    }
  }

  /*
   * Get aggregate columns used in the select query
   */

  /**
   * Gets the aggregate columns.
   *
   * @param node
   *          the node
   * @return the aggregate columns
   */
  public ArrayList<String> getAggregateColumns(ASTNode node) {

    StringBuilder aggmeasures = new StringBuilder();
    if (HQLParser.isAggregateAST(node)) {
      if (node.getToken().getType() == HiveParser.TOK_FUNCTION || node.getToken().getType() == HiveParser.DOT) {

        ASTNode right = (ASTNode) node.getChild(1);
        String aggCol = HQLParser.getString((ASTNode) right);

        String funident = HQLParser.findNodeByPath(node, Identifier).toString();
        String measure = funident.concat("(").concat(aggCol).concat(")");

        String alias = measure.replaceAll("\\s+", "").replaceAll("\\(\\(", "_").replaceAll("[.]", "_")
            .replaceAll("[)]", "");
        String allaggmeasures = aggmeasures.append(measure).append(" as ").append(alias).toString();
        String aggColAlias = funident.toString().concat("(").concat(alias).concat(")");

        mapAggTabAlias.put(measure, aggColAlias);
        if (!aggColumn.contains(allaggmeasures)) {
          aggColumn.add(allaggmeasures);
        }
      }
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      getAggregateColumns(child);
    }
    return (ArrayList<String>) aggColumn;
  }

  /*
   * Get all columns in table.column format
   */

  /**
   * Gets the tables and columns.
   *
   * @param node
   *          the node
   * @return the tables and columns
   */
  public ArrayList<String> getTablesAndColumns(ASTNode node) {

    if (node.getToken().getType() == HiveParser.DOT) {
      String table = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL, Identifier).toString();
      String column = node.getChild(1).toString().toLowerCase();
      String keys = table.concat(".").concat(column);
      allkeys.add(keys);
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      getTablesAndColumns(child);
    }
    return (ArrayList<String>) allkeys;
  }

  /*
   * Get the limit value
   */

  /**
   * Gets the limit clause.
   *
   * @param node
   *          the node
   * @return the limit clause
   */
  public String getLimitClause(ASTNode node) {

    if (node.getToken().getType() == HiveParser.TOK_LIMIT) {
      limit = HQLParser.findNodeByPath(node, HiveParser.Number).toString();
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      getLimitClause(child);
    }
    return limit;
  }

  /*
   * Get all filters conditions in where clause
   */

  /**
   * Gets the all filters.
   *
   * @param node
   *          the node
   * @return the all filters
   */
  public void getAllFilters(ASTNode node) {
    if (node == null) {
      return;
    }
    if (node.getToken().getType() == HiveParser.KW_AND) {
      ASTNode right = (ASTNode) node.getChild(1);
      String allFilters = HQLParser.getString((ASTNode) right);
      leftFilter = HQLParser.getString((ASTNode) node.getChild(0));
      rightFilter.add(allFilters);
    } else if (node.getToken().getType() == HiveParser.TOK_WHERE) {
      ASTNode right = (ASTNode) node.getChild(1);
      String allFilters = HQLParser.getString((ASTNode) right);
      leftFilter = HQLParser.getString((ASTNode) node.getChild(0));
      rightFilter.add(allFilters);
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      getAllFilters(child);
    }
  }

  /*
   * Get the fact table name and alias
   */
  /**
   * Gets the fact name alias.
   *
   * @param fromAST
   *          the from ast
   * @return the fact name alias
   */
  public String getFactNameAlias(ASTNode fromAST) {
    String factTable;
    String factAlias;
    ArrayList<String> allTables = new ArrayList<String>();
    getAllTablesfromFromAST(fromAST, allTables);

    String[] keys = allTables.get(0).trim().split(" +");
    if (keys.length == 2) {
      factTable = keys[0];
      factAlias = keys[1];
      return factTable + " " + factAlias;
    } else {
      factTable = keys[0];
    }
    return factTable;
  }

  /*
   * Reset the instance variables if input query is union of multiple select queries
   */

  /**
   * Reset.
   */
  public void reset() {
    factInLineQuery.setLength(0);
    factKeys.setLength(0);
    aggColumn.clear();
    allSubQueries.setLength(0);
    rightFilter.clear();
    joinCondition.setLength(0);
    selectTree = fromTree = joinTree = whereTree = groupByTree = havingTree = orderByTree = null;
    selectAST = fromAST = joinAST = whereAST = groupByAST = havingAST = orderByAST = null;
    limit = null;
  }

  /*
   * Check the incompatible hive udf and replace it with database udf.
   */

  /**
   * Replace udf for db.
   *
   * @param query
   *          the query
   * @return the string
   */
  public String replaceUDFForDB(String query) {
    Map<String, String> imputnmatch = new HashMap<String, String>();
    imputnmatch.put("to_date", "date");
    imputnmatch.put("format_number", "format");
    imputnmatch.put("date_sub\\((.*?),\\s*([0-9]+\\s*)\\)", "date_sub($1, interval $2 day)");
    imputnmatch.put("date_add\\((.*?),\\s*([0-9]+\\s*)\\)", "date_add($1, interval $2 day)");

    for (Map.Entry<String, String> entry : imputnmatch.entrySet()) {
      query = query.replaceAll(entry.getKey(), entry.getValue());
    }
    return query;
  }

  /*
   * Construct the rewritten query using trees
   */

  /**
   * Builds the query.
   *
   * @throws SemanticException
   *           the semantic exception
   * @throws HiveException
   *           the hive exception
   */
  public void buildQuery() throws SemanticException, HiveException {
    analyzeInternal();
    CubeMetastoreClient client = CubeMetastoreClient.getInstance(new HiveConf(conf, ColumnarSQLRewriter.class));
    replaceWithUnderlyingStorage(fromAST, client);
    fromTree = HQLParser.getString(fromAST);

    getFilterInJoinCond(fromAST);
    getAggregateColumns(selectAST);
    getJoinCond(fromAST);
    getAllFilters(whereAST);
    buildSubqueries(fromAST);

    // Get the limit clause
    String limit = getLimitClause(ast);

    // Construct the final fact in-line query with keys,
    // measures and individual sub queries built.

    if (whereTree == null || joinTree == null || allSubQueries.length() == 0) {
      LOG.info("@@@Query not eligible for inner subquery rewrite");
      // construct query without fact sub query
      constructQuery(selectTree, fromTree, whereTree, groupByTree, havingTree, orderByTree, limit, joinTree);
      return;
    } else {
      String factNameAndAlias = getFactNameAlias(fromAST).trim();
      factInLineQuery.append(" (select ").append(factKeys);
      if (!aggColumn.isEmpty()) {
        factInLineQuery.append(aggColumn.toString().replace("[", "").replace("]", ""));
      }
      if (factInLineQuery.toString().substring(factInLineQuery.toString().length() - 1).equals(",")) {
        factInLineQuery.setLength(factInLineQuery.length() - 1);
      }
      factInLineQuery.append(" from ").append(factNameAndAlias);
      if (allSubQueries != null) {
        factInLineQuery.append(" where ");
        factInLineQuery.append(allSubQueries.toString().substring(0, allSubQueries.lastIndexOf("and")));
      }
      if (!aggColumn.isEmpty()) {
        factInLineQuery.append(" group by ");
        factInLineQuery.append(factKeys.toString().substring(0, factKeys.toString().lastIndexOf(",")));
      }
      factInLineQuery.append(")");
    }

    // Replace the aggregate column aliases from fact
    // sub query query to the outer query

    for (Map.Entry<String, String> entry : mapAggTabAlias.entrySet()) {
      selectTree = selectTree.replace(entry.getKey(), entry.getValue());
      if (orderByTree != null) {
        orderByTree = orderByTree.replace(entry.getKey(), entry.getValue());
      }
      if (havingTree != null) {
        havingTree = havingTree.replace(entry.getKey(), entry.getValue());
      }
    }
    // construct query with fact sub query
    constructQuery(selectTree, fromTree, whereTree, groupByTree, havingTree, orderByTree, limit, joinTree);

  }

  /*
   * Get first child from the from tree
   */
  /**
   * Gets the all tablesfrom from ast.
   *
   * @param from
   *          the from
   * @param fromTables
   *          the from tables
   * @return the all tablesfrom from ast
   */
  private void getAllTablesfromFromAST(ASTNode from, ArrayList<String> fromTables) {
    String table = "";
    if (TOK_TABREF == from.getToken().getType()) {
      ASTNode tabName = (ASTNode) from.getChild(0);
      if (tabName.getChildCount() == 2) {
        table = tabName.getChild(0).getText() + "." + tabName.getChild(1).getText();
      } else {
        table = tabName.getChild(0).getText();
      }
      if (from.getChildCount() > 1) {
        table = table + " " + from.getChild(1).getText();
      }
      fromTables.add(table);
    }

    for (int i = 0; i < from.getChildCount(); i++) {
      ASTNode child = (ASTNode) from.getChild(i);
      getAllTablesfromFromAST(child, fromTables);
    }
  }

  /*
   * Construct final query using all trees
   */
  /**
   * Construct query.
   *
   * @param selecttree
   *          the selecttree
   * @param fromtree
   *          the fromtree
   * @param wheretree
   *          the wheretree
   * @param groupbytree
   *          the groupbytree
   * @param havingtree
   *          the havingtree
   * @param orderbytree
   *          the orderbytree
   * @param limit
   *          the limit
   * @param jointree
   *          the jointree
   */
  private void constructQuery(String selecttree, String fromtree, String wheretree, String groupbytree,
      String havingtree, String orderbytree, String limit, String jointree) {

    String finalJoinClause = "";
    String factNameAndAlias = getFactNameAlias(fromAST);

    if (joinCondition != null) {
      finalJoinClause = factNameAndAlias.concat(" ").concat(joinCondition.toString());
    } else {
      finalJoinClause = factNameAndAlias;
    }
    rewrittenQuery.append("select ").append(selecttree).append(" from ");
    if (factInLineQuery.length() != 0) {
      rewrittenQuery.append(finalJoinClause.replaceFirst(factNameAndAlias.substring(0, factNameAndAlias.indexOf(' ')),
          factInLineQuery.toString()));
    } else {
      rewrittenQuery.append(finalJoinClause);
    }
    if (wheretree != null) {
      rewrittenQuery.append(" where ").append(wheretree);
    }
    if (groupbytree != null) {
      rewrittenQuery.append(" group by ").append(groupbytree);
    }
    if (havingtree != null) {
      rewrittenQuery.append(" having ").append(havingtree);
    }
    if (orderbytree != null) {
      rewrittenQuery.append(" order by ").append(orderbytree);
    }
    if (limit != null) {
      rewrittenQuery.append(" limit ").append(limit);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.driver.jdbc.QueryRewriter#rewrite(org.apache.hadoop.conf.Configuration, java.lang.String)
   */
  @Override
  public synchronized String rewrite(Configuration conf, String query) throws LensException {
    this.query = query;
    this.conf = conf;
    StringBuilder mergedQuery = new StringBuilder();
    rewrittenQuery.setLength(0);
    String queryReplacedUdf = "";
    reset();

    try {
      if (query.toLowerCase().matches("(.*)union all(.*)")) {
        String finalRewrittenQuery = "";
        String[] queries = query.toLowerCase().split("union all");
        for (int i = 0; i < queries.length; i++) {
          LOG.info("Union Query Part " + i + " : " + queries[i]);
          ast = HQLParser.parseHQL(queries[i]);
          buildQuery();
          mergedQuery = rewrittenQuery.append(" union all ");
          finalRewrittenQuery = mergedQuery.toString().substring(0, mergedQuery.lastIndexOf("union all"));
          reset();
        }
        queryReplacedUdf = replaceUDFForDB(finalRewrittenQuery);
        LOG.info("Input Query : " + query);
        LOG.info("Rewritten Query :  " + queryReplacedUdf);
      } else {
        ast = HQLParser.parseHQL(query);
        buildQuery();
        queryReplacedUdf = replaceUDFForDB(rewrittenQuery.toString());
        LOG.info("Input Query : " + query);
        LOG.info("Rewritten Query :  " + queryReplacedUdf);
      }
    } catch (ParseException e) {
      throw new LensException(e);
    } catch (SemanticException e) {
      throw new LensException(e);
    } catch (HiveException e) {
      throw new LensException(e);
    }
    return queryReplacedUdf;
  }

  // Replace Lens database names with storage's proper DB and table name based
  // on table properties.
  /**
   * Replace with underlying storage.
   *
   * @param tree
   *          the tree
   * @param metastoreClient
   *          the metastore client
   */
  protected void replaceWithUnderlyingStorage(ASTNode tree, CubeMetastoreClient metastoreClient) {
    if (tree == null) {
      return;
    }

    if (TOK_TABNAME == tree.getToken().getType()) {
      // If it has two children, the first one is the DB name and second one is
      // table identifier
      // Else, we have to add the DB name as the first child
      try {
        if (tree.getChildCount() == 2) {
          ASTNode dbIdentifier = (ASTNode) tree.getChild(0);
          ASTNode tableIdentifier = (ASTNode) tree.getChild(1);
          String lensTable = tableIdentifier.getText();
          String table = getUnderlyingTableName(metastoreClient, lensTable);
          String db = getUnderlyingDBName(metastoreClient, lensTable);

          // Replace both table and db names
          if ("default".equalsIgnoreCase(db)) {
            // Remove the db name for this case
            tree.deleteChild(0);
          } else if (StringUtils.isNotBlank(db)) {
            dbIdentifier.getToken().setText(db);
          } // If db is empty, then leave the tree untouched

          if (StringUtils.isNotBlank(table)) {
            tableIdentifier.getToken().setText(table);
          }
        } else {
          ASTNode tableIdentifier = (ASTNode) tree.getChild(0);
          String lensTable = tableIdentifier.getText();
          String table = getUnderlyingTableName(metastoreClient, lensTable);
          // Replace table name
          if (StringUtils.isNotBlank(table)) {
            tableIdentifier.getToken().setText(table);
          }

          // Add db name as a new child
          String dbName = getUnderlyingDBName(metastoreClient, lensTable);
          if (StringUtils.isNotBlank(dbName) && !"default".equalsIgnoreCase(dbName)) {
            ASTNode dbIdentifier = new ASTNode(new CommonToken(HiveParser.Identifier, dbName));
            dbIdentifier.setParent(tree);
            tree.insertChild(0, dbIdentifier);
          }
        }
      } catch (HiveException e) {
        LOG.warn("No corresponding table in metastore:" + e.getMessage());
      }
    } else {
      for (int i = 0; i < tree.getChildCount(); i++) {
        replaceWithUnderlyingStorage((ASTNode) tree.getChild(i), metastoreClient);
      }
    }
  }

  /**
   * Gets the underlying db name.
   *
   * @param client
   *          the client
   * @param table
   *          the table
   * @return the underlying db name
   * @throws HiveException
   *           the hive exception
   */
  String getUnderlyingDBName(CubeMetastoreClient client, String table) throws HiveException {
    Table tbl = client.getHiveTable(table);
    return tbl == null ? null : tbl.getProperty(LensConfConstants.NATIVE_DB_NAME);
  }

  /**
   * Gets the underlying table name.
   *
   * @param client
   *          the client
   * @param table
   *          the table
   * @return the underlying table name
   * @throws HiveException
   *           the hive exception
   */
  String getUnderlyingTableName(CubeMetastoreClient client, String table) throws HiveException {
    Table tbl = client.getHiveTable(table);
    return tbl == null ? null : tbl.getProperty(LensConfConstants.NATIVE_TABLE_NAME);
  }

}
