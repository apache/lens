package com.inmobi.grill.driver.jdbc;

/*
 * #%L
 * Grill Driver for JDBC
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.inmobi.grill.server.api.GrillConfConstants;
import org.antlr.runtime.CommonToken;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.parse.CubeSemanticAnalyzer;
import org.apache.hadoop.hive.ql.cube.parse.HQLParser;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import com.inmobi.grill.api.GrillException;

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

public class ColumnarSQLRewriter implements QueryRewriter {
  private Configuration conf;
  private String clauseName = null;
  private QB qb;
  protected ASTNode ast;
  protected String query;
  private String finalFactQuery;
  private String limit;
  private StringBuilder factFilters = new StringBuilder();
  private StringBuilder factInLineQuery = new StringBuilder();

  protected StringBuilder allSubQueries = new StringBuilder();
  protected StringBuilder factKeys = new StringBuilder();
  protected StringBuilder rewrittenQuery = new StringBuilder();
  protected StringBuilder mergedQuery = new StringBuilder();
  protected String finalRewrittenQuery;

  protected StringBuilder joinCondition = new StringBuilder();
  protected List<String> allkeys = new ArrayList<String>();
  protected List<String> aggColumn = new ArrayList<String>();
  protected List<String> filterInJoinCond = new ArrayList<String>();
  protected List<String> rightFilter = new ArrayList<String>();

  private String leftFilter;
  private Map<String, String> mapAggTabAlias = new HashMap<String, String>();
  private static final Log LOG = LogFactory.getLog(ColumnarSQLRewriter.class);

  private String factTable;
  private String factAlias;
  private String whereTree;
  private String havingTree;
  private String orderByTree;
  private String selectTree;
  private String groupByTree;
  private String joinTree;

  private ASTNode joinAST;
  private ASTNode havingAST;
  private ASTNode selectAST;
  private ASTNode whereAST;
  private ASTNode orderByAST;
  private ASTNode groupByAST;
  protected ASTNode fromAST;

  public ColumnarSQLRewriter(HiveConf conf, String query) {
    // super(conf);
    this.conf = conf;
    this.query = query;
  }

  public ColumnarSQLRewriter() {
    // super(conf);
  }

  public String getClause() {
    if (clauseName == null) {
      TreeSet<String> ks = new TreeSet<String>(qb.getParseInfo()
          .getClauseNames());
      clauseName = ks.first();
    }
    return clauseName;
  }

  // Analyze query AST and split into sub trees
  public void analyzeInternal() throws SemanticException {
    HiveConf conf = new HiveConf();
    CubeSemanticAnalyzer c1 = new CubeSemanticAnalyzer(conf);

    QB qb = new QB(null, null, false);

    if (!c1.doPhase1(ast, qb, c1.initPhase1Ctx())) {
      return;
    }

    // Get clause name
    TreeSet<String> ks = new TreeSet<String>(qb.getParseInfo().getClauseNames());
    clauseName = ks.first();

    // Split query into trees
    if (qb.getParseInfo().getWhrForClause(clauseName) != null) {
      this.whereTree = HQLParser.getString(qb.getParseInfo().getWhrForClause(
          clauseName));
      this.whereAST = qb.getParseInfo().getWhrForClause(clauseName);
    }

    if (qb.getParseInfo().getHavingForClause(clauseName) != null) {
      this.havingTree = HQLParser.getString(qb.getParseInfo()
          .getHavingForClause(clauseName));
      this.havingAST = qb.getParseInfo().getHavingForClause(clauseName);
    }

    if (qb.getParseInfo().getOrderByForClause(clauseName) != null) {
      this.orderByTree = HQLParser.getString(qb.getParseInfo()
          .getOrderByForClause(clauseName));
      this.orderByAST = qb.getParseInfo().getOrderByForClause(clauseName);
    }
    if (qb.getParseInfo().getGroupByForClause(clauseName) != null) {
      this.groupByTree = HQLParser.getString(qb.getParseInfo()
          .getGroupByForClause(clauseName));
      this.groupByAST = qb.getParseInfo().getGroupByForClause(clauseName);
    }

    if (qb.getParseInfo().getSelForClause(clauseName) != null) {
      this.selectTree = HQLParser.getString(qb.getParseInfo().getSelForClause(
          clauseName));
      this.selectAST = qb.getParseInfo().getSelForClause(clauseName);
    }

    this.joinTree = HQLParser.getString(qb.getParseInfo().getJoinExpr());
    this.joinAST = qb.getParseInfo().getJoinExpr();

    this.fromAST = HQLParser.findNodeByPath(ast, TOK_FROM);
    getFactNameAlias();
  }

  // Get join conditions
  public void getJoinCond(ASTNode node) {

    if (node == null) {
      return;
    }

    try {
      int rootType = node.getToken().getType();
      if (rootType == TOK_JOIN || rootType == TOK_LEFTOUTERJOIN
          || rootType == TOK_RIGHTOUTERJOIN || rootType == TOK_FULLOUTERJOIN
          || rootType == TOK_LEFTSEMIJOIN || rootType == TOK_UNIQUEJOIN) {

        ASTNode left = (ASTNode) node.getChild(0);
        ASTNode right = (ASTNode) node.getChild(1);

        String joinType = "";
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
        String joinCond = "";

        if (node.getChildCount() > 2) {
          // User has specified a join condition for filter pushdown.
          joinCond = HQLParser.getString((ASTNode) node.getChild(2));
        }

        String joinClause = joinType.concat(HQLParser.getString(right))
            .concat(" on ").concat(joinCond);
        joinCondition.append(" ").append(joinClause);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      getJoinCond(child);
    }
  }

  // Get filter conditions if user has specified a join
  // condition for filter pushdown.
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

  // match join tree with where tree to build subquery
  public void buildSubqueries(ASTNode node) {
    if (node == null) {
      LOG.debug("Join AST is null " + node);
      return;
    }

    String subquery = "";
    if (node.getToken().getType() == HiveParser.EQUAL) {
      if (node.getChild(0).getType() == HiveParser.DOT
          && node.getChild(1).getType() == HiveParser.DOT) {

        ASTNode left = (ASTNode) node.getChild(0);
        ASTNode right = (ASTNode) node.getChild(1);

        // Get the fact and dimension columns in table_name.column_name format
        String factJoinKeys = HQLParser.getString((ASTNode) left).toString()
            .replaceAll("\\s+", "").replaceAll("[(,)]", "");
        String dimJoinKeys = HQLParser.getString((ASTNode) right).toString()
            .replaceAll("\\s+", "").replaceAll("[(,)]", "");

        String dimTableName = dimJoinKeys
            .substring(0, dimJoinKeys.indexOf("."));
        factKeys.append(factJoinKeys).append(",");

        // Construct part of subquery by referring join condition   
        // fact.fact_key = dim_table.dim_key
        // eg. "fact_key in ( select dim_key from dim_table where "
        String queryphase1 = factJoinKeys.concat(" in ").concat(" ( ")
            .concat(" select ").concat(dimJoinKeys).concat(" from ")
            .concat(dimTableName).concat(" where ");

        getAllFilters(whereAST);
        rightFilter.add(leftFilter);

        Set<String> setAllFilters = new HashSet<String>(rightFilter);

        // Check the occurrence of dimension table in the filter list and
        // combine all filters of same dimension table with and .
        // eg. "dim_table.key1 = 'abc' and dim_table.key2 = 'xyz'"
        if (setAllFilters.toString().matches(
            "(.*)".toString().concat(dimTableName).concat("(.*)"))) {

          factFilters.delete(0, factFilters.length());

          // All filters in where clause
          for (int i = 0; i < setAllFilters.toArray().length; i++) {
            if (setAllFilters.toArray() != null) {
              if (setAllFilters.toArray()[i].toString().matches(
                  "(.*)".toString().concat(dimTableName).concat("(.*)"))) {
                String filters2 = setAllFilters.toArray()[i].toString().concat(
                    " and ");
                factFilters.append(filters2);
              }
            }
          }

          // Merge fact subquery and dim subqury to construct the final subquery
          // eg. "fact_key in ( select dim_key from dim_table where
          // dim_table.key2 = 'abc' and dim_table.key3 = 'xyz'"
          subquery = queryphase1.concat(
              factFilters.toString().substring(0,
                  factFilters.toString().lastIndexOf("and"))).concat(")");
          allSubQueries.append(subquery).append(" and ");
        }
      }
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      buildSubqueries(child);
    }
  }

  // Get aggregate columns
  public ArrayList<String> getAggregateColumns(ASTNode node) {
    
    StringBuilder aggmeasures = new StringBuilder();
    if (HQLParser.isAggregateAST(node)) {
      if (node.getToken().getType() == HiveParser.TOK_FUNCTION
          || node.getToken().getType() == HiveParser.DOT) {

        ASTNode right = (ASTNode) node.getChild(1);
        String aggCol = HQLParser.getString((ASTNode) right);

        String funident = HQLParser.findNodeByPath(node, Identifier).toString();
        String measure = funident.concat("(").concat(aggCol).concat(")");

        String alias = measure.replaceAll("\\s+", "").replaceAll("\\(\\(", "_")
            .replaceAll("[.]", "_").replaceAll("[)]", "");
        String allaggmeasures = aggmeasures.append(measure).append(" as ")
            .append(alias).toString();
        String aggColAlias = funident.toString().concat("(").concat(alias)
            .concat(")");

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

  // Get all columns in table.column format for fact table
  public ArrayList<String> getTablesAndColumns(ASTNode node) {

    if (node.getToken().getType() == HiveParser.DOT) {
      String table = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL,
          Identifier).toString();
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

  // Get the limit value
  public String getLimitClause(ASTNode node) {

    if (node.getToken().getType() == HiveParser.TOK_LIMIT)
      limit = HQLParser.findNodeByPath(node, HiveParser.Number).toString();

    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      getLimitClause(child);
    }
    return limit;
  }

  // Get all filters conditions in where clause
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

  // Get the fact table name and alias
  public void getFactNameAlias() {
    Pattern p = Pattern.compile("from(.*)");
    Matcher m = p.matcher(query.toLowerCase());
    if (m.find()) {
      String fromClause = m.group(1);
      String[] keys = fromClause.split(" ");
      factTable = keys[1];
      factAlias = keys[2];
    }
  }

  public void reset() {
    factInLineQuery.delete(0, factInLineQuery.length());
    factKeys.delete(0, factKeys.length());
    aggColumn.clear();
    factTable = "";
    factAlias = "";
    allSubQueries.delete(0, allSubQueries.length());
    rightFilter.clear();
    joinCondition.delete(0, joinCondition.length());
  }

  public void buildQuery() {

    try {
      analyzeInternal();
    } catch (SemanticException e) {
      e.printStackTrace();
    }
    try {
      CubeMetastoreClient client = CubeMetastoreClient.getInstance(new HiveConf(conf, ColumnarSQLRewriter.class));
      replaceWithUnderlyingStorage(fromAST, client);
    } catch (HiveException exc) {
      LOG.error("Error replacing DB & column names", exc);
    }

    getFilterInJoinCond(joinAST);
    getAggregateColumns(selectAST);
    getJoinCond(joinAST);
    getAllFilters(whereAST);
    buildSubqueries(joinAST);

    // Construct the final fact in-line query with keys,
    // measures and
    // individual sub queries built.

    if (whereTree == null || joinTree == null) {
      LOG.info("No filters specified in where clause. Nothing to rewrite");
      LOG.info("Input Query : " + query);
      LOG.info("Rewritten Query : " + query);
      finalRewrittenQuery = query;

    } else {
      factInLineQuery.append(" (select ").append(factKeys)
          .append(aggColumn.toString().replace("[", "").replace("]", ""))
          .append(" from ").append(factTable).append(" ").append(factAlias);
      if (allSubQueries != null)
        factInLineQuery.append(" where ");
      factInLineQuery.append(allSubQueries.toString().substring(0,
          allSubQueries.lastIndexOf("and")));
      factInLineQuery.append(" group by ");
      factInLineQuery.append(factKeys.toString().substring(0,
          factKeys.toString().lastIndexOf(",")));
      factInLineQuery.append(")");
    }

    // Replace the aggregate column aliases from fact
    // in-line query to the outer query

    for (Map.Entry<String, String> entry : mapAggTabAlias.entrySet()) {
      selectTree = selectTree.replace(entry.getKey(), entry.getValue());
      if (orderByTree != null) {
        orderByTree = orderByTree.replace(entry.getKey(), entry.getValue());
      }
      if (havingTree != null) {
        havingTree = havingTree.replace(entry.getKey(), entry.getValue());
      }
    }

    // Get the limit clause
    String limit = getLimitClause(ast);

    // Add fact table and alias to the join query
    String finalJoinClause = factTable.concat(" ").concat(factAlias)
        .concat(joinCondition.toString());

    // Construct the final rewritten query
    rewrittenQuery
        .append("select ")
        .append(selectTree)
        .append(" from ")
        .append(
            finalJoinClause.replaceAll(factTable, factInLineQuery.toString()))
        .append(" where ").append(whereTree).append(" group by ")
        .append(groupByTree);
    if (havingTree != null)
      rewrittenQuery.append(" having ").append(havingTree);
    if (orderByTree != null)
      rewrittenQuery.append(" order by ").append(orderByTree);
    if (limit != null)

      rewrittenQuery.append(" limit ").append(limit);

  }

  @Override
  public synchronized String rewrite(Configuration conf, String query)
      throws GrillException {
    this.query = query;
    this.conf = conf;

    try {
      if (query.toLowerCase().matches("(.*)union all(.*)")) {
        String[] queries = query.toLowerCase().split("union all");
        for (int i = 0; i < queries.length; i++) {
          ast = HQLParser.parseHQL(queries[i]);
          buildQuery();
          mergedQuery = rewrittenQuery.append(" union all ");
          finalRewrittenQuery = mergedQuery.toString().substring(0,
              mergedQuery.lastIndexOf("union all"));
          LOG.info("Input Query : " + query);
          LOG.info("Rewritten Query :  " + finalRewrittenQuery);
          reset();
        }
      } else {
        ast = HQLParser.parseHQL(query);
        buildQuery();
        finalRewrittenQuery = rewrittenQuery.toString();
        LOG.info("Input Query : " + query);
        LOG.info("Rewritten Query :  " + finalRewrittenQuery);
      }
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return finalRewrittenQuery;
  }

  // Replace Grill database names with storage's proper DB and table name based on table properties.
  protected  void replaceWithUnderlyingStorage(ASTNode tree, CubeMetastoreClient metastoreClient) {
    if (tree == null) {
      return;
    }

    if (TOK_TABNAME == tree.getToken().getType()) {
      // If it has two children, the first one is the DB name and second one is table identifier
      // Else, we have to add the DB name as the first child
      try {
        if (tree.getChildCount() == 2) {
          ASTNode dbIdentifier = (ASTNode) tree.getChild(0);
          ASTNode tableIdentifier = (ASTNode) tree.getChild(1);
          String grillTable = tableIdentifier.getText();
          String table = getUnderlyingTableName(metastoreClient, grillTable);
          String db = getUnderlyingDBName(metastoreClient, grillTable);

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
          String grillTable = tableIdentifier.getText();
          String table = getUnderlyingTableName(metastoreClient, grillTable);
          // Replace table name
          if (StringUtils.isNotBlank(table)) {
            tableIdentifier.getToken().setText(table);
          }

          // Add db name as a new child
          String dbName = getUnderlyingDBName(metastoreClient, grillTable);
          if (StringUtils.isNotBlank(dbName) && !"default".equalsIgnoreCase(dbName)) {
            ASTNode dbIdentifier = new ASTNode(new CommonToken(HiveParser.Identifier,
              dbName));
            dbIdentifier.setParent(tree);
            tree.insertChild(0, dbIdentifier);
          }
        }
      } catch (HiveException exc) {
        LOG.error("Error replacing db & table names", exc);
      }
    } else {
      for (int i = 0; i < tree.getChildCount(); i++) {
        replaceWithUnderlyingStorage((ASTNode) tree.getChild(i), metastoreClient);
      }
    }
  }

  String getUnderlyingDBName(CubeMetastoreClient client, String table) throws HiveException {
    Table tbl = client.getHiveTable(table);
    return tbl == null ? null : tbl.getProperty(GrillConfConstants.GRILL_NATIVE_DB_NAME);
  }

  String getUnderlyingTableName(CubeMetastoreClient client, String table) throws HiveException {
    Table tbl = client.getHiveTable(table);
    return tbl == null ? null : tbl.getProperty(GrillConfConstants.GRILL_NATIVE_TABLE_NAME);
  }

}