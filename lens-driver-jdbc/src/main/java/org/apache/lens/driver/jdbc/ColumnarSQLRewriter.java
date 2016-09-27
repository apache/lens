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

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

import java.util.*;
import java.util.regex.Pattern;

import org.apache.lens.api.util.CommonUtils;
import org.apache.lens.cube.metadata.CubeMetastoreClient;
import org.apache.lens.cube.parse.CubeSemanticAnalyzer;
import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.rewrite.QueryRewriter;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import org.antlr.runtime.CommonToken;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * The Class ColumnarSQLRewriter.
 */
@Slf4j
public class ColumnarSQLRewriter implements QueryRewriter {

  /** The clause name. */
  protected String clauseName = null;

  /** The qb. */
  protected QB qb;

  /** The ast. */
  protected ASTNode ast;

  /** The query. */
  protected String query;

  /** The limit. */
  protected String limit;

  /** The fact filters. */
  protected final StringBuilder factFilters = new StringBuilder();

  /** The fact in line query. */
  protected final StringBuilder factInLineQuery = new StringBuilder();

  /** The all sub queries. */
  protected StringBuilder allSubQueries = new StringBuilder();

  /** The fact keys. */
  Set<String> factKeys = new LinkedHashSet<String>();

  /** The rewritten query. */
  protected StringBuilder rewrittenQuery = new StringBuilder();

  /** The fact filters for push down */
  protected StringBuilder factFilterPush = new StringBuilder();

  /** The join list. */
  protected List<String> joinList = new ArrayList<String>();

  /** The join condition. */
  protected StringBuilder joinCondition = new StringBuilder();

  /** The allkeys. */
  protected List<String> allkeys = new ArrayList<String>();

  /** The agg column. */
  protected List<String> aggColumn = new ArrayList<String>();

  /** The right filter. */
  protected List<String> rightFilter = new ArrayList<String>();

  /** The left filter. */
  protected String leftFilter;

  /** The map agg tab alias. */
  private final Map<String, String> mapAggTabAlias = new LinkedHashMap<String, String>();

  /** The map aliases. */
  private final Map<String, String> mapAliases = new LinkedHashMap<String, String>();

  /** The table to alias map. */
  private final Map<String, String> tableToAliasMap = new LinkedHashMap<String, String>();

  /** The tables to accessed column map. */
  private final Map<String, LinkedHashSet<String>> tableToAccessedColMap =
    new LinkedHashMap<String, LinkedHashSet<String>>();

  /** The dimension table to subquery map. */
  private final Map<String, String> dimTableToSubqueryMap = new LinkedHashMap<String, String>();

  /** The where tree. */
  protected String whereTree;

  /** The having tree. */
  protected String havingTree;

  /** The order by tree. */
  protected String orderByTree;

  /** The select tree. */
  protected String selectTree;

  /** The group by tree. */
  protected String groupByTree;

  /** The join tree. */
  protected String joinTree;

  /** The from tree. */
  protected String fromTree;

  /** The join ast. */
  @Getter
  protected ASTNode joinAST;

  /** The having ast. */
  @Getter
  protected ASTNode havingAST;

  /** The select ast. */
  @Getter
  protected ASTNode selectAST;

  /** The where ast. */
  @Getter
  protected ASTNode whereAST;

  /** The order by ast. */
  @Getter
  protected ASTNode orderByAST;

  /** The group by ast. */
  @Getter
  protected ASTNode groupByAST;

  /** The from ast. */
  @Getter
  protected ASTNode fromAST;
  protected Map<String, String> regexReplaceMap;

  /**
   * Instantiates a new columnar sql rewriter.
   */
  public ColumnarSQLRewriter() {
  }

  @Override
  public void init(Configuration conf) {
    regexReplaceMap = CommonUtils.parseMapFromString(conf.get(JDBCDriverConfConstants.REGEX_REPLACEMENT_VALUES));
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
   * @param tree the tree
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
    String[] tabSplit = table.split(" +");

    if (tabSplit.length == 2) {
      tableToAliasMap.put(tabSplit[0], tabSplit[1]);
    }
    return table;
  }

  /*
   * Get join conditions specified in join clause
   */

  /**
   * Gets the join cond.
   *
   * @param node the node
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
      getAllDimColumns(fromAST);
      getAllDimColumns(selectAST);
      getAllDimColumns(whereAST);

      buildDimSubqueries();
      // Get the table from input db.table alias.
      // If alias provided put the same alias in the subquery.
      String[] tabSplit = rightTable.split(" +");
      String subqueryForTable = "";
      if (tabSplit.length == 2) {
        subqueryForTable = dimTableToSubqueryMap.get(tabSplit[0]) + " " + tabSplit[1];
      } else {
        subqueryForTable = dimTableToSubqueryMap.get(tabSplit[0]);
      }
      String joinType = "";
      String joinFilter = "";
      String joinToken = node.getToken().getText();

      if (joinToken.equals("TOK_JOIN")) {
        joinType = "inner join";
      } else if (joinToken.equals("TOK_LEFTOUTERJOIN")) {
        joinType = "left outer join";
      } else if (joinToken.equals("TOK_RIGHTOUTERJOIN")) {
        joinType = "right outer join";
      } else if (joinToken.equals("TOK_FULLOUTERJOIN")) {
        joinType = "full outer join";
      } else if (joinToken.equals("TOK_LEFTSEMIJOIN")) {
        joinType = "left semi join";
      } else if (joinToken.equals("TOK_UNIQUEJOIN")) {
        joinType = "unique join";
      } else {
        log.info("Non supported join type : {}", joinToken);
      }

      if (node.getChildCount() > 2) {
        // User has specified a join condition for filter pushdown.
        joinFilter = HQLParser.getString((ASTNode) node.getChild(2));
      }
      joinList.add(joinType + (" ") + (subqueryForTable) + (" on ") + (joinFilter) + (" "));
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      getJoinCond(child);
    }
  }

  /**
   * Construct join chain
   *
   * @return
   */
  public StringBuilder constructJoinChain() {
    getJoinCond(fromAST);
    Collections.reverse(joinList);

    for (String key : joinList) {
      joinCondition.append(" ").append(key);
    }
    return joinCondition;
  }

  /**
   * Get the count of columns in a given select expression
   *
   * @param node
   * @return Column count
   */
  public int getColumnCount(ASTNode node) {
    int count = 0;
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      if (child.getToken().getType() == TOK_TABLE_OR_COL) {
        count++;
      } else {
        count += getColumnCount(child);
      }
    }
    return count;
  }

  /**
   * Check if expression is answerable from fact, then push it to fact pushdown subquery
   *
   * @param node
   * @return true if expressions is used
   */
  public boolean isExpressionsAnswerableFromFact(ASTNode node) {
    boolean isAnswerable = true;
    for (int i = 0; i < node.getChildCount(); i++) {
      if (node.getChild(i).getType() == HiveParser.TOK_SELEXPR) {
        int cnt = getColumnCount((ASTNode) node.getChild(i));
        if (cnt >= 2) {
          if (cnt == getNumFactTableInExpressions((ASTNode) node.getChild(i), new MutableInt(0))) {
            isAnswerable = true;
          } else {
            isAnswerable = false;
          }
        }
      }
    }
    return isAnswerable;
  }

  /**
   * Get number of fact columns used in the an expression
   *
   * @param node
   * @param count
   * @return Number of fact columns used in expression
   */
  protected int getNumFactTableInExpressions(ASTNode node, MutableInt count) {

    if (node == null) {
      log.debug("ASTNode is null ");
      return 0;
    }
    if (node.getToken().getType() == HiveParser.TOK_TABLE_OR_COL) {
      String factAlias = getFactAlias();
      String table = node.getChild(0).getText();
      if (table.equals(factAlias)) {
        count.add(1);
      }
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      getNumFactTableInExpressions(child, count);
    }

    return count.intValue();
  }


  /*
   * Get filter conditions if user has specified a join condition for filter pushdown.
   */

  /**
   * Gets the filter in join cond.
   *
   * @param node the node
   */
  public void getFilterInJoinCond(ASTNode node) {

    if (node == null) {
      log.debug("Join AST is null ");
      return;
    }

    if (node.getToken().getType() == HiveParser.KW_AND) {
      ASTNode right = (ASTNode) node.getChild(1);
      String filterCond = HQLParser.getString(right);
      rightFilter.add(filterCond);
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      getFilterInJoinCond(child);
    }
  }

  /**
   * Get the fact alias
   *
   * @return
   */

  public String getFactAlias() {
    String factAlias = "";
    String factNameAndAlias = getFactNameAlias(fromAST);
    String[] keys = factNameAndAlias.split("\\s+");
    if (keys.length == 2) {
      factAlias = keys[1];
    }
    return factAlias;
  }

  /**
   * Get fact filters for pushdown
   *
   * @param node
   */

  public void factFilterPushDown(ASTNode node) {
    if (node == null) {
      log.debug("Join AST is null ");
      return;
    }

    String filterCond = "";
    if (node.getToken().getType() == HiveParser.KW_AND) {

      ASTNode parentNode = (ASTNode) node.getChild(0).getParent();
      // Skip the join conditions used as "and" for fact filter pushdown.
      // eg. inner join fact.id1 = dim.id and fact.id2 = dim.id
      if (parentNode.getChild(0).getChild(0).getType() == HiveParser.DOT
        && parentNode.getChild(0).getChild(1).getType() == HiveParser.DOT
        && parentNode.getChild(1).getChild(0).getType() == HiveParser.DOT
        && parentNode.getChild(1).getChild(1).getType() == HiveParser.DOT) {
        return;
      }
      ASTNode right = (ASTNode) node.getChild(1);
      filterCond = HQLParser.getString(right);
    }
    String factAlias = getFactAlias();

    if (filterCond.matches("(.*)" + factAlias + "(.*)")) {
      factFilterPush.append(filterCond).append(" and ");
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      factFilterPushDown(child);
    }
  }

  /**
   * Get fact keys used in the AST
   *
   * @param node
   */
  public void getFactKeysFromNode(ASTNode node) {
    if (node == null) {
      log.debug("AST is null ");
      return;
    }
    if (HQLParser.isAggregateAST(node)) {
      return;
    } else {
      if (node.getToken().getType() == HiveParser.DOT
              && node.getParent().getChild(0).getType() != HiveParser.Identifier) {
        String table = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL, Identifier).toString();
        String column = node.getChild(1).toString().toLowerCase();

        String factAlias = getFactAlias();

        if (table.equals(factAlias)) {
          factKeys.add(factAlias + "." + column);
        }
      }
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      getFactKeysFromNode(child);
    }
  }

  /**
   * Get all fact keys used in all ASTs
   */
  public void getAllFactKeys() {
    if (fromAST != null) {
      getFactKeysFromNode(fromAST);
    }
    if (whereAST != null) {
      getFactKeysFromNode(whereAST);
    }
    if (selectAST != null) {
      getFactKeysFromNode(selectAST);
    }
  }

  /**
   *  Get all columns used for dimmension tables
   * @param node
   */
  public void getAllDimColumns(ASTNode node) {

    if (node == null) {
      log.debug("Input AST is null ");
      return;
    }
    // Assuming column is specified with table.column format
    if (node.getToken().getType() == HiveParser.DOT) {
      String table = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL, Identifier).toString();
      String column = node.getChild(1).toString();

      Iterator iterator = tableToAliasMap.keySet().iterator();
      while (iterator.hasNext()) {
        String tab = (String) iterator.next();
        String alias = tableToAliasMap.get(tab);

        if ((table.equals(tab) || table.equals(alias)) && column != null) {
          LinkedHashSet<String> cols;
          if (!tableToAccessedColMap.containsKey(tab)) {
            cols = new LinkedHashSet<String>();
            cols.add(column);
            tableToAccessedColMap.put(tab, cols);
          } else {
            cols = tableToAccessedColMap.get(tab);
            if (!cols.contains(column)) {
              cols.add(column);
            }
          }
        }
      }
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      getAllDimColumns(child);
    }
  }

  /**
   * Build dimension table subqueries
   */
  public void buildDimSubqueries() {
    Iterator iterator = tableToAccessedColMap.keySet().iterator();
    while (iterator.hasNext()) {
      StringBuilder query = new StringBuilder();
      String tab = (String) iterator.next();
      LinkedHashSet<String> cols = tableToAccessedColMap.get(tab);
      query.append("(").append("select ").append(StringUtils.join(cols, ","))
          .append(" from ").append(tab).append(")");
      dimTableToSubqueryMap.put(tab, query.toString());
    }
  }

  /*
   * Build fact sub query using where tree and join tree
   */

  /**
   * Builds the subqueries.
   *
   * @param node the node
   */
  public void buildSubqueries(ASTNode node) {
    if (node == null) {
      log.debug("Join AST is null ");
      return;
    }

    String subquery = "";
    if (node.getToken().getType() == HiveParser.EQUAL) {
      if (node.getChild(0).getType() == HiveParser.DOT && node.getChild(1).getType() == HiveParser.DOT) {

        ASTNode left = (ASTNode) node.getChild(0);
        ASTNode right = (ASTNode) node.getChild(1);

        ASTNode parentNode = (ASTNode) node.getParent();

        // Skip the join conditions used as "and" while building subquery
        // eg. inner join fact.id1 = dim.id and fact.id2 = dim.id
        if (parentNode.getChild(0).getChild(0).getType() == HiveParser.DOT
          && parentNode.getChild(0).getChild(1).getType() == HiveParser.DOT
          && parentNode.getChild(1).getChild(0).getType() == HiveParser.DOT
          && parentNode.getChild(1).getChild(1).getType() == HiveParser.DOT) {
          return;
        }

        // Get the fact and dimension columns in table_name.column_name format
        String factJoinKeys = HQLParser.getString(left).replaceAll("\\s+", "")
          .replaceAll("[(,)]", "");
        String dimJoinKeys = HQLParser.getString(right).replaceAll("\\s+", "")
          .replaceAll("[(,)]", "");
        int dimTableDelimIndex = dimJoinKeys.indexOf("__");
        String dimTableName = dimJoinKeys.substring(0, dimTableDelimIndex);
        String dimAlias = dimJoinKeys.
            substring(dimTableDelimIndex + 3, dimJoinKeys.indexOf('.')).trim();

        // Construct part of subquery by referring join condition
        // fact.fact_key = dim_table.dim_key
        // eg. "fact_key in ( select dim_key from dim_table where "
        String queryphase1 = factJoinKeys.concat(" in ").concat(" ( ").concat(" select ")
          .concat(dimTableName).concat(" ")
          .concat(dimJoinKeys.substring(dimJoinKeys.lastIndexOf(".")))
          .concat(" from ").concat(dimTableName).concat(" where ");

        getAllFilters(whereAST);
        rightFilter.add(leftFilter);

        Set<String> setAllFilters = new LinkedHashSet<String>(rightFilter);

        // Check the occurrence of dimension table in the filter list and
        // combine all filters of same dimension table with and .
        // eg. "dim_table.key1 = 'abc' and dim_table.key2 = 'xyz'"
        if (setAllFilters.toString().replaceAll("\\s+", "")
            .matches("(.*)" + dimAlias + "(.*)")) {

          factFilters.delete(0, factFilters.length());

          // All filters in where clause
          for (int i = 0; i < setAllFilters.toArray().length; i++) {

            if (setAllFilters.toArray()[i].toString().replaceAll("\\s+", "")
                .matches("(.*)" + dimAlias + ("(.*)"))) {
              String filters2 = setAllFilters.toArray()[i].toString();
              filters2 = filters2.replaceAll(
                getTableOrAlias(filters2, "alias"),
                getTableOrAlias(filters2, "table")
              ).concat(" and ");
              factFilters.append(filters2);
            }
          }
          // Merge fact subquery and dim subqury to construct the final subquery
          // eg. "fact_key in ( select dim_key from dim_table where
          // dim_table.key2 = 'abc' and dim_table.key3 = 'xyz'"
          subquery = queryphase1.concat(factFilters.toString().substring(0, factFilters.toString().lastIndexOf("and")))
            .concat(")");
          // include subqueries which are applicable only to filter records from fact
          if (subquery.matches("(.*)" + getFactAlias() + "(.*)")) {
            allSubQueries.append(subquery).append(" and ");
          }
        }
      }
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      buildSubqueries(child);
    }
  }

  /**
   * Get the table or alias from the given key string
   *
   * @param keyString
   * @param type
   * @return
   */

  public String getTableOrAlias(String keyString, String type) {
    String ref = "";
    if (type.equals("table")) {
      ref = keyString.substring(0, keyString.indexOf("__")).replaceAll("[(,)]", "");
    }
    if (type.equals("alias")) {
      ref = keyString.substring(0, keyString.indexOf(".")).replaceAll("[(,)]", "");
    }
    return ref;
  }

  /*
   * Get aggregate columns used in the select query
   */

  /**
   * Gets the aggregate columns.
   *
   * @param node the node
   * @return the aggregate columns
   */
  public ArrayList<String> getAggregateColumns(ASTNode node, MutableInt count) {

    StringBuilder aggmeasures = new StringBuilder();
    if (HQLParser.isAggregateAST(node)) {
      if (node.getToken().getType() == HiveParser.TOK_FUNCTION || node.getToken().getType() == HiveParser.DOT) {

        ASTNode right = (ASTNode) node.getChild(1);
        String aggCol = HQLParser.getString(right);

        String funident = HQLParser.findNodeByPath(node, Identifier).toString();
        String measure = funident.concat("(").concat(aggCol).concat(")");
        count.add(1);
        String alias = "alias" + String.valueOf(count);
        String allaggmeasures = aggmeasures.append(measure).append(" as ").append(alias).toString();
        String aggColAlias = funident + "(" + alias + ")";
        String measureRegex = "\\s*" + Pattern.quote(funident)
          + "\\s*\\(\\s*\\Q" + aggCol.replaceAll("\\s+", "\\\\E\\\\s+\\\\Q") + "\\E\\s*\\)\\s*";
        mapAggTabAlias.put(measureRegex, aggColAlias);
        if (!aggColumn.contains(allaggmeasures)) {
          aggColumn.add(allaggmeasures);
        }
      }
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      getAggregateColumns(child, count);
    }
    return (ArrayList<String>) aggColumn;
  }

  /*
   * Get all columns in table.column format
   */

  /**
   * Gets the tables and columns.
   *
   * @param node the node
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
   * @param node the node
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
   * @param node the node
   */
  public void getAllFilters(ASTNode node) {
    if (node == null) {
      return;
    }
    if (node.getToken().getType() == HiveParser.KW_AND) {
      ASTNode right = (ASTNode) node.getChild(1);
      String allFilters = HQLParser.getString(right);
      leftFilter = HQLParser.getString((ASTNode) node.getChild(0));
      rightFilter.add(allFilters);
    } else if (node.getToken().getType() == HiveParser.TOK_WHERE) {
      ASTNode right = (ASTNode) node.getChild(1);
      String allFilters = HQLParser.getString(right);
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
   * @param fromAST the from ast
   * @return the fact name alias
   */
  public String getFactNameAlias(ASTNode fromAST) {
    String factTable;
    String factAlias;
    ArrayList<String> allTables = new ArrayList<>();
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
    factKeys.clear();
    aggColumn.clear();
    allSubQueries.setLength(0);
    factFilterPush.setLength(0);
    rightFilter.clear();
    joinCondition.setLength(0);
    tableToAliasMap.clear();
    tableToAccessedColMap.clear();
    dimTableToSubqueryMap.clear();

    selectTree = null;
    selectAST = null;

    fromTree = null;
    fromAST = null;

    joinTree = null;
    joinAST = null;

    whereTree = null;
    whereAST = null;

    groupByTree = null;
    groupByAST = null;

    havingTree = null;
    havingAST = null;

    orderByTree = null;
    orderByAST = null;

    mapAliases.clear();
    joinList.clear();
    limit = null;
  }

  /*
   * Check the incompatible hive udf and replace it with database udf.
   */

  /**
   * Replace udf for db.
   *
   * @param query the query
   * @return the string
   */
  public String replaceUDFForDB(String query) {
    for (Map.Entry<String, String> entry : regexReplaceMap.entrySet()) {
      query = query.replaceAll(entry.getKey(), entry.getValue());
    }
    return query;
  }

  /**
   * Replace alias in AST trees
   *
   */

  public void replaceAliasInAST() {
    updateAliasFromAST(fromAST);
    if (fromTree != null) {
      replaceAlias(fromAST);
      fromTree = HQLParser.getString(fromAST);
    }
    if (selectTree != null) {
      replaceAlias(selectAST);
      selectTree = HQLParser.getString(selectAST);
    }
    if (whereTree != null) {
      replaceAlias(whereAST);
      whereTree = HQLParser.getString(whereAST);
    }
    if (groupByTree != null) {
      replaceAlias(groupByAST);
      groupByTree = HQLParser.getString(groupByAST);
    }
    if (orderByTree != null) {
      replaceAlias(orderByAST);
      orderByTree = HQLParser.getString(orderByAST);
    }
    if (havingTree != null) {
      replaceAlias(havingAST);
      havingTree = HQLParser.getString(havingAST);
    }
  }

  /*
   * Construct the rewritten query using trees
   */

  /**
   * Builds the query.
   *
   * @throws SemanticException
   */
  public void buildQuery(Configuration conf, HiveConf hconf) throws SemanticException {
    analyzeInternal(conf, hconf);
    replaceWithUnderlyingStorage(hconf);
    replaceAliasInAST();
    getFilterInJoinCond(fromAST);
    MutableInt alaisCount = new MutableInt(0);
    getAggregateColumns(selectAST, alaisCount);
    if (havingAST != null) {
      getAggregateColumns(havingAST, alaisCount);
    }
    constructJoinChain();
    getAllFilters(whereAST);
    buildSubqueries(fromAST);
    getAllFactKeys();
    factFilterPushDown(whereAST);
    factFilterPushDown(fromAST);


    // Get the limit clause
    String limit = getLimitClause(ast);

    // Construct the final fact in-line query with keys,
    // measures and individual sub queries built.

    if (whereTree == null || joinTree == null || allSubQueries.length() == 0
        || aggColumn.isEmpty() || !isExpressionsAnswerableFromFact(selectAST)) {
      log.info("@@@Query not eligible for inner subquery rewrite");
      // construct query without fact sub query
      constructQuery(selectTree, whereTree, groupByTree, havingTree, orderByTree, limit);
      return;
    } else {
      String factNameAndAlias = getFactNameAlias(fromAST).trim();
      factInLineQuery.append(" (select ").append(factKeys.toString().replaceAll("\\[", "").replaceAll("\\]", ""));
      if (!aggColumn.isEmpty()) {
        factInLineQuery.append(",").append(aggColumn.toString().replace("[", "").replace("]", ""));
      }
      if (factInLineQuery.toString().substring(factInLineQuery.toString().length() - 1).equals(",")) {
        factInLineQuery.setLength(factInLineQuery.length() - 1);
      }
      factInLineQuery.append(" from ").append(factNameAndAlias);
      if (allSubQueries != null) {
        factInLineQuery.append(" where ");
        if (factFilterPush != null) {
          factInLineQuery.append(factFilterPush);
        }
        factInLineQuery.append(allSubQueries.toString().substring(0, allSubQueries.lastIndexOf("and")));
      }
      if (!aggColumn.isEmpty()) {
        factInLineQuery.append(" group by ");
        factInLineQuery.append(factKeys.toString().replaceAll("\\[", "").replaceAll("\\]", ""));
      }
      factInLineQuery.append(")");
    }

    // Replace the aggregate column aliases from fact
    // sub query query to the outer query

    for (Map.Entry<String, String> entry : mapAggTabAlias.entrySet()) {
      selectTree = selectTree.replaceAll(entry.getKey(), entry.getValue());

      if (orderByTree != null) {
        orderByTree = orderByTree.replaceAll(entry.getKey(), entry.getValue());
      }
      if (havingTree != null) {
        havingTree = havingTree.replaceAll(entry.getKey(), entry.getValue());
      }
    }
    //for subquery with count function should be replaced with sum in outer query
    if (selectTree.toLowerCase().matches("(.*)count\\((.*)")) {
      selectTree = selectTree.replaceAll("count\\(", "sum\\(");
    }
    // construct query with fact sub query
    constructQuery(selectTree, whereTree, groupByTree, havingTree, orderByTree, limit);

  }

  /*
   * Get first child from the from tree
   */

  /**
   * Gets the all tablesfrom from ast.
   *
   * @param from       the from
   * @param fromTables the from tables
   * @return the all tablesfrom from ast
   */
  protected void getAllTablesfromFromAST(ASTNode from, ArrayList<String> fromTables) {
    String table;
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

  /**
   * Update alias and map old alias with new one
   *
   * @param from
   */
  protected void updateAliasFromAST(ASTNode from) {

    String newAlias;
    String table;
    String dbAndTable = "";
    if (TOK_TABREF == from.getToken().getType()) {
      ASTNode tabName = (ASTNode) from.getChild(0);
      if (tabName.getChildCount() == 2) {
        dbAndTable = tabName.getChild(0).getText() + "_" + tabName.getChild(1).getText();
        table = tabName.getChild(1).getText();
      } else {
        table = tabName.getChild(0).getText();
      }
      if (from.getChildCount() > 1) {
        ASTNode alias = (ASTNode) from.getChild(1);
        newAlias = dbAndTable + "_" + from.getChild(1).getText();
        mapAliases.put(alias.getText(), table + "__" + newAlias);
        alias.getToken().setText(table + "__" + newAlias);
      }
    }
    for (int i = 0; i < from.getChildCount(); i++) {
      updateAliasFromAST((ASTNode) from.getChild(i));

    }
  }

  /**
   * Update alias in all AST trees
   *
   * @param tree
   */
  protected void replaceAlias(ASTNode tree) {
    if (TOK_TABLE_OR_COL == tree.getToken().getType()) {
      ASTNode alias = (ASTNode) tree.getChild(0);
      if (mapAliases.get(tree.getChild(0).toString()) != null) {
        alias.getToken().setText(mapAliases.get(tree.getChild(0).toString()));
      } else {
        alias.getToken().setText(tree.getChild(0).toString());
      }
    }
    for (int i = 0; i < tree.getChildCount(); i++) {
      replaceAlias((ASTNode) tree.getChild(i));
    }
  }

  /*
   * Construct final query using all trees
   */

  /**
   * Construct query.
   *
   * @param selecttree  the selecttree
   * @param wheretree   the wheretree
   * @param groupbytree the groupbytree
   * @param havingtree  the havingtree
   * @param orderbytree the orderbytree
   * @param limit       the limit
   */
  protected void constructQuery(String selecttree, String wheretree, String groupbytree,
    String havingtree, String orderbytree, String limit) {

    String finalJoinClause = "";
    String factNameAndAlias = getFactNameAlias(fromAST);

    if (joinCondition != null) {
      finalJoinClause = factNameAndAlias.concat(" ").concat(joinCondition.toString());
    } else {
      finalJoinClause = factNameAndAlias;
    }
    rewrittenQuery.append("select ").append(selecttree).append(" from ");
    if (factInLineQuery.length() != 0) {
      finalJoinClause = finalJoinClause.substring(finalJoinClause.indexOf(" "));
      rewrittenQuery.append(factInLineQuery);
      rewrittenQuery.append(finalJoinClause);
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
   * @see org.apache.lens.server.api.query.QueryRewriter#rewrite(java.lang.String, org.apache.hadoop.conf.Configuration)
   */
  @Override
  public String rewrite(String query, Configuration conf, HiveConf metastoreConf) throws LensException {
    this.query = query;
    StringBuilder mergedQuery;
    rewrittenQuery.setLength(0);
    String queryReplacedUdf;
    reset();

    try {
      String finalRewrittenQuery;
      if (query.toLowerCase().matches("(.*)union all(.*)")) {
        finalRewrittenQuery = "";
        String[] queries = query.toLowerCase().split("union all");
        for (int i = 0; i < queries.length; i++) {
          log.info("Union Query Part {} : {}", i, queries[i]);
          ast = HQLParser.parseHQL(queries[i], metastoreConf);
          buildQuery(conf, metastoreConf);
          mergedQuery = rewrittenQuery.append(" union all ");
          finalRewrittenQuery = mergedQuery.toString().substring(0, mergedQuery.lastIndexOf("union all"));
          reset();
        }
      } else {
        ast = HQLParser.parseHQL(query, metastoreConf);
        buildQuery(conf, metastoreConf);
        finalRewrittenQuery = rewrittenQuery.toString();
      }
      queryReplacedUdf = replaceUDFForDB(finalRewrittenQuery);
      log.info("Input Query : {}", query);
      log.info("Rewritten Query : {}", queryReplacedUdf);
    } catch (SemanticException e) {
      throw new LensException(e);
    }
    return queryReplacedUdf;
  }


  @NoArgsConstructor
  protected static class NativeTableInfo {
    private Map<String, String> columnMapping = new LinkedHashMap<>();
    NativeTableInfo(Table tbl) {
      String columnMappingProp = tbl.getProperty(LensConfConstants.NATIVE_TABLE_COLUMN_MAPPING);
      if (StringUtils.isNotBlank(columnMappingProp)) {
        String[] columnMapArray = StringUtils.split(columnMappingProp, ",");
        for (String columnMapEntry : columnMapArray) {
          String[] mapEntry = StringUtils.split(columnMapEntry, "=");
          columnMapping.put(mapEntry[0].trim(), mapEntry[1].trim());
        }
      }
    }
    String getNativeColumn(String col) {
      String retCol = columnMapping.get(col);
      return retCol != null ? retCol : col;
    }
  }

  protected Map<String, NativeTableInfo> aliasToNativeTableInfo = new LinkedHashMap<>();

  /**
   * Replace with underlying storage.
   *
   * @param metastoreConf the metastore configuration
   */
  protected void replaceWithUnderlyingStorage(HiveConf metastoreConf) {
    replaceDBAndTableNames(metastoreConf, fromAST);
    if (aliasToNativeTableInfo.isEmpty()) {
      return;
    }
    replaceColumnNames(selectAST);
    replaceColumnNames(fromAST);
    replaceColumnNames(whereAST);
    replaceColumnNames(groupByAST);
    replaceColumnNames(orderByAST);
    replaceColumnNames(havingAST);
  }
  // Replace Lens database names with storage's proper DB and table name based
  // on table properties.
  protected void replaceDBAndTableNames(HiveConf metastoreConf, ASTNode tree) {
    if (tree == null) {
      return;
    }

    if (TOK_TABREF == tree.getToken().getType()) {
      // TOK_TABREF will have TOK_TABNAME as first child and alias as second child.
      String alias;
      String tblName = null;
      Table tbl = null;
      ASTNode tabNameChild = (ASTNode) tree.getChild(0);
      if (TOK_TABNAME == tabNameChild.getToken().getType()) {
        // If it has two children, the first one is the DB name and second one is
        // table identifier
        // Else, we have to add the DB name as the first child
        try {
          if (tabNameChild.getChildCount() == 2) {
            ASTNode dbIdentifier = (ASTNode) tabNameChild.getChild(0);
            ASTNode tableIdentifier = (ASTNode) tabNameChild.getChild(1);
            tblName = tableIdentifier.getText();
            String lensTable = dbIdentifier.getText() + "." + tblName;
            tbl = CubeMetastoreClient.getInstance(metastoreConf).getHiveTable(lensTable);
            String table = getUnderlyingTableName(tbl);
            String db = getUnderlyingDBName(tbl);

            // Replace both table and db names
            if ("default".equalsIgnoreCase(db)) {
              // Remove the db name for this case
              tabNameChild.deleteChild(0);
            } else if (StringUtils.isNotBlank(db)) {
              dbIdentifier.getToken().setText(db);
            } // If db is empty, then leave the tree untouched

            if (StringUtils.isNotBlank(table)) {
              tableIdentifier.getToken().setText(table);
            }
          } else {
            ASTNode tableIdentifier = (ASTNode) tabNameChild.getChild(0);
            tblName = tableIdentifier.getText();
            tbl = CubeMetastoreClient.getInstance(metastoreConf).getHiveTable(tblName);
            String table = getUnderlyingTableName(tbl);
            // Replace table name
            if (StringUtils.isNotBlank(table)) {
              tableIdentifier.getToken().setText(table);
            }

            // Add db name as a new child
            String dbName = getUnderlyingDBName(tbl);
            if (StringUtils.isNotBlank(dbName) && !"default".equalsIgnoreCase(dbName)) {
              ASTNode dbIdentifier = new ASTNode(new CommonToken(HiveParser.Identifier, dbName));
              dbIdentifier.setParent(tabNameChild);
              tabNameChild.insertChild(0, dbIdentifier);
            }
          }
        } catch (LensException | HiveException e) {
          log.warn("No corresponding table in metastore:", e);
        }
      }
      if (tree.getChildCount() == 2) {
        alias = tree.getChild(1).getText();
      } else {
        alias = tblName;
      }
      if (StringUtils.isNotBlank(alias)) {
        alias = alias.toLowerCase();
        if (!aliasToNativeTableInfo.containsKey(alias)) {
          if (tbl != null) {
            aliasToNativeTableInfo.put(alias, new NativeTableInfo(tbl));
          }
        }
      }
    } else {
      for (int i = 0; i < tree.getChildCount(); i++) {
        replaceDBAndTableNames(metastoreConf, (ASTNode) tree.getChild(i));
      }
    }
  }

  void replaceColumnNames(ASTNode node) {
    if (node == null) {
      return;
    }
    int nodeType = node.getToken().getType();
    if (nodeType == HiveParser.DOT) {
      ASTNode tabident = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL, Identifier);
      ASTNode colIdent = (ASTNode) node.getChild(1);
      String column = colIdent.getText().toLowerCase();
      String alias = tabident.getText().toLowerCase();
      if (aliasToNativeTableInfo.get(alias) != null) {
        colIdent.getToken().setText(aliasToNativeTableInfo.get(alias).getNativeColumn(column));
      }
    } else {
      // recurse down
      for (int i = 0; i < node.getChildCount(); i++) {
        ASTNode child = (ASTNode) node.getChild(i);
        replaceColumnNames(child);
      }
    }
  }

  /**
   * Gets the underlying db name.
   *
   * @param tbl the table
   * @return the underlying db name
   * @throws HiveException the hive exception
   */
  String getUnderlyingDBName(Table tbl) throws HiveException {
    return tbl == null ? null : tbl.getProperty(LensConfConstants.NATIVE_DB_NAME);
  }

  /**
   * Gets the underlying table name.
   *
   * @param tbl the table
   * @return the underlying table name
   * @throws HiveException the hive exception
   */
  String getUnderlyingTableName(Table tbl) throws HiveException {
    return tbl == null ? null : tbl.getProperty(LensConfConstants.NATIVE_TABLE_NAME);
  }

}
