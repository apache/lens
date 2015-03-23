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
package org.apache.lens.driver.cube;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lens.api.LensException;
import org.apache.lens.cube.parse.CubeQueryContext;
import org.apache.lens.cube.parse.CubeQueryRewriter;
import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.metrics.MethodMetricsContext;
import org.apache.lens.server.api.metrics.MethodMetricsFactory;
import org.apache.lens.server.api.query.AbstractQueryContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.log4j.Logger;

import lombok.Getter;

/**
 * The Class RewriteUtil.
 */
public final class RewriteUtil {
  private RewriteUtil() {

  }
  public static final Logger LOG = Logger.getLogger(RewriteUtil.class);

  /** The cube pattern. */
  static Pattern cubePattern = Pattern.compile(".*CUBE(.*)", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE
    | Pattern.DOTALL);

  /** The matcher. */
  static Matcher matcher = null;

  /**
   * The Class CubeQueryInfo.
   */
  static class CubeQueryInfo {

    /** The start pos. */
    int startPos;

    /** The end pos. */
    int endPos;

    /** The query. */
    String query;

    /** The cube ast. */
    ASTNode cubeAST;
  }

  /**
   * Find cube positions.
   *
   * @param query the query
   * @return the list
   * @throws SemanticException the semantic exception
   * @throws ParseException    the parse exception
   */
  static List<CubeQueryInfo> findCubePositions(String query, HiveConf conf) throws SemanticException, ParseException {
    ASTNode ast = HQLParser.parseHQL(query, conf);
    LOG.debug("User query AST:" + ast.dump());
    List<CubeQueryInfo> cubeQueries = new ArrayList<CubeQueryInfo>();
    findCubePositions(ast, cubeQueries, query);
    for (CubeQueryInfo cqi : cubeQueries) {
      cqi.query = query.substring(cqi.startPos, cqi.endPos);
    }
    return cubeQueries;
  }

  /**
   * Find cube positions.
   *
   * @param ast           the ast
   * @param cubeQueries   the cube queries
   * @param originalQuery the original query
   * @throws SemanticException the semantic exception
   */
  private static void findCubePositions(ASTNode ast, List<CubeQueryInfo> cubeQueries, String originalQuery)
    throws SemanticException {
    int childCount = ast.getChildCount();
    if (ast.getToken() != null) {
      if (ast.getChild(0) != null) {
        LOG.debug("First child:" + ast.getChild(0) + " Type:"
          + ((ASTNode) ast.getChild(0)).getToken().getType());
      }
      if (ast.getToken().getType() == HiveParser.TOK_QUERY
        && ((ASTNode) ast.getChild(0)).getToken().getType() == HiveParser.KW_CUBE) {
        LOG.debug("Inside cube clause");
        CubeQueryInfo cqi = new CubeQueryInfo();
        cqi.cubeAST = ast;
        if (ast.getParent() != null) {
          ASTNode parent = (ASTNode) ast.getParent();
          cqi.startPos = ast.getCharPositionInLine();
          int ci = ast.getChildIndex();
          if (parent.getToken() == null || parent.getToken().getType() == HiveParser.TOK_EXPLAIN
            || parent.getToken().getType() == HiveParser.TOK_CREATETABLE) {
            // Not a sub query
            cqi.endPos = originalQuery.length();
          } else if (parent.getChildCount() > ci + 1) {
            if (parent.getToken().getType() == HiveParser.TOK_SUBQUERY) {
              // less for the next start and for close parenthesis
              cqi.endPos = getEndPos(originalQuery, parent.getChild(ci + 1).getCharPositionInLine(), ")");
            } else if (parent.getToken().getType() == HiveParser.TOK_UNION) {
              // one less for the next start and less the size of string 'UNION ALL'
              cqi.endPos = getEndPos(originalQuery, parent.getChild(ci + 1).getCharPositionInLine() - 1, "UNION ALL");
            } else {
              // Not expected to reach here
              LOG.warn("Unknown query pattern found with AST:" + ast.dump());
              throw new SemanticException("Unknown query pattern");
            }
          } else {
            // last child of union all query
            // one for next AST
            // and one for the close parenthesis if there are no more unionall
            // or one for the string 'UNION ALL' if there are more union all
            LOG.debug("Child of union all");
            cqi.endPos = getEndPos(originalQuery, parent.getParent().getChild(1).getCharPositionInLine(), ")",
              "UNION ALL");
          }
        }
        LOG.debug("Adding cqi " + cqi + " query:" + originalQuery.substring(cqi.startPos, cqi.endPos));
        cubeQueries.add(cqi);
      } else {
        for (int childPos = 0; childPos < childCount; ++childPos) {
          findCubePositions((ASTNode) ast.getChild(childPos), cubeQueries, originalQuery);
        }
      }
    } else {
      LOG.warn("Null AST!");
    }
  }

  /**
   * Gets the end pos.
   *
   * @param query          the query
   * @param backTrackIndex the back track index
   * @param backTrackStr   the back track str
   * @return the end pos
   */
  private static int getEndPos(String query, int backTrackIndex, String... backTrackStr) {
    if (backTrackStr != null) {
      String q = query.substring(0, backTrackIndex).toLowerCase();
      for (int i = 0; i < backTrackStr.length; i++) {
        if (q.trim().endsWith(backTrackStr[i].toLowerCase())) {
          backTrackIndex = q.lastIndexOf(backTrackStr[i].toLowerCase());
          break;
        }
      }
    }
    while (Character.isSpaceChar(query.charAt(backTrackIndex - 1))) {
      backTrackIndex--;
    }
    return backTrackIndex;
  }

  /**
   * Gets the rewriter.
   *
   * @param queryConf the query conf
   * @return the rewriter
   * @throws SemanticException the semantic exception
   */
  static CubeQueryRewriter getCubeRewriter(Configuration queryConf, HiveConf hconf) throws SemanticException {
    return new CubeQueryRewriter(queryConf, hconf);
  }

  /**
   * Replaces new lines with spaces; '&&' with AND; '||' with OR // these two can be removed once HIVE-5326 gets
   * resolved.
   *
   * @param query the query
   * @return the replaced query
   */
  static String getReplacedQuery(final String query) {
    return query.replaceAll("[\\n\\r]", " ").replaceAll("&&", " AND ").replaceAll("\\|\\|", " OR ").trim();
  }

  private static final String REWRITE_QUERY_GAUGE = RewriteUtil.class.getSimpleName() + "-rewriteQuery";
  private static final String TOHQL_GAUGE = RewriteUtil.class.getSimpleName() + "-rewriteQuery-toHQL";

  /**
   * Rewrite query.
   *
   * @param ctx the query context
   * @return the map
   * @throws LensException the lens exception
   */
  public static Map<LensDriver, DriverRewriterRunnable> rewriteQuery(AbstractQueryContext ctx) throws LensException {
    try {
      String replacedQuery = getReplacedQuery(ctx.getUserQuery());
      Map<LensDriver, DriverRewriterRunnable> runnables = new LinkedHashMap<LensDriver, DriverRewriterRunnable>();
      List<RewriteUtil.CubeQueryInfo> cubeQueries = findCubePositions(replacedQuery, ctx.getHiveConf());

      for (LensDriver driver : ctx.getDriverContext().getDrivers()) {
        runnables.put(driver, new DriverRewriterRunnable(driver, ctx, cubeQueries, replacedQuery));
      }

      return runnables;
    } catch (Exception e) {
      throw new LensException("Rewriting failed, cause :" + e.getMessage(), e);
    }
  }

  public static class DriverRewriterRunnable implements Runnable {
    @Getter
    private final LensDriver driver;
    private final AbstractQueryContext ctx;
    private final List<CubeQueryInfo> cubeQueries;
    private final String replacedQuery;

    @Getter
    /** Indicate if rewrite operation succeeded */
    private boolean succeeded;

    @Getter
    /** Get cause of rewrite failure if rewrite operation failed */
    private String failureCause = null;

    @Getter
    /** Get eventual rewritten query */
    private String rewrittenQuery;

    public DriverRewriterRunnable(LensDriver driver,
                                  AbstractQueryContext ctx,
                                  List<CubeQueryInfo> cubeQueries,
                                  String replacedQuery) {
      this.driver = driver;
      this.ctx = ctx;
      this.cubeQueries = cubeQueries;
      this.replacedQuery = replacedQuery;
    }

    @Override
    public void run() {
      String lowerCaseQuery = replacedQuery.toLowerCase();
      if (lowerCaseQuery.startsWith("add") || lowerCaseQuery.startsWith("set")) {
        rewrittenQuery = replacedQuery;
        return;
      }

      MethodMetricsContext rewriteGauge = MethodMetricsFactory.createMethodGauge(ctx.getDriverConf(driver), true,
        REWRITE_QUERY_GAUGE);
      StringBuilder builder = new StringBuilder();
      int start = 0;
      CubeQueryRewriter rewriter = null;
      try {
        if (cubeQueries.size() > 0) {
          // avoid creating rewriter if there are no cube queries
          rewriter = getCubeRewriter(ctx.getDriverContext().getDriverConf(driver), ctx.getHiveConf());
          ctx.setOlapQuery(true);
        }

        // We have to rewrite each sub cube query which might be present in the original
        // user query. We are looping through all sub queries here.
        int qIndex = 1;
        for (RewriteUtil.CubeQueryInfo cqi : cubeQueries) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Rewriting cube query:" + cqi.query);
          }

          if (start != cqi.startPos) {
            builder.append(replacedQuery.substring(start, cqi.startPos));
          }

          // Parse and rewrite individual cube query
          CubeQueryContext cqc = rewriter.rewrite(cqi.query);
          MethodMetricsContext toHQLGauge = MethodMetricsFactory.createMethodGauge(ctx.getDriverConf(driver), true,
            qIndex + "-" + TOHQL_GAUGE);
          // toHQL actually generates the rewritten query
          String hqlQuery = cqc.toHQL();
          toHQLGauge.markSuccess();
          qIndex++;

          if (LOG.isDebugEnabled()) {
            LOG.debug("Rewritten query:" + hqlQuery);
          }

          builder.append(hqlQuery);
          start = cqi.endPos;
        }

        builder.append(replacedQuery.substring(start));

        rewrittenQuery = builder.toString();
        succeeded = true;
        ctx.setDriverQuery(driver, rewrittenQuery);
        LOG.info("Final rewritten query for driver:" + driver + " is: " + rewrittenQuery);
      } catch (Exception e) {
        // we are catching all exceptions sothat other drivers can be picked in case of driver bugs
        LOG.warn("Driver : " + driver + " Skipped for the query rewriting due to ", e);
        ctx.setDriverRewriteError(driver, e);
        failureCause = new StringBuilder(" Driver :")
          .append(driver.getClass().getName())
          .append(" Cause :" + e.getLocalizedMessage())
          .toString();
      } finally {
        if (rewriter != null) {
          rewriter.clear();
        }
        rewriteGauge.markSuccess();
      }
    }

    @Override
    public String toString() {
      return "Rewrite runnable for " + driver;
    }
  }

  /**
   * Checks if is cube query.
   *
   * @param query the query
   * @return true, if is cube query
   */
  public static boolean isCubeQuery(String query) {
    if (matcher == null) {
      matcher = cubePattern.matcher(query);
    } else {
      matcher.reset(query);
    }
    return matcher.matches();
  }

}
