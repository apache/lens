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
package org.apache.lens.server.rewrite;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lens.cube.metadata.CubeMetastoreClient;
import org.apache.lens.cube.parse.CubeQueryContext;
import org.apache.lens.cube.parse.CubeQueryRewriter;
import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.driver.cube.RewriterPlan;
import org.apache.lens.server.api.driver.DriverQueryPlan;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.metrics.MethodMetricsContext;
import org.apache.lens.server.api.metrics.MethodMetricsFactory;
import org.apache.lens.server.api.query.AbstractQueryContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * The Class RewriteUtil.
 */
@Slf4j
public final class RewriteUtil {
  private RewriteUtil() {

  }
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
   * @throws LensException     the lensexception
   */
  static List<CubeQueryInfo> findCubePositions(String query, HiveConf conf)
    throws LensException {

    ASTNode ast = HQLParser.parseHQL(query, conf);
    if (log.isDebugEnabled()) {
      log.debug("User query AST:{}", ast.dump());
    }
    List<CubeQueryInfo> cubeQueries = new ArrayList<>();
    findCubePositions(ast, cubeQueries, query, conf);
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
   * @throws LensException the lens exception
   */
  private static void findCubePositions(ASTNode ast, List<CubeQueryInfo> cubeQueries, String originalQuery,
    HiveConf conf)
    throws LensException {
    int childCount = ast.getChildCount();
    if (ast.getToken() != null) {
      if (log.isDebugEnabled() && ast.getChild(0) != null) {
        log.debug("First child: {} Type:{}", ast.getChild(0), ast.getChild(0).getType());
      }
      if (ast.getType() == HiveParser.TOK_QUERY
        && (isCubeKeywordNode((ASTNode) ast.getChild(0)) || isFromNodeWithCubeTable((ASTNode) ast.getChild(0), conf))) {
        log.debug("Inside cube clause");
        CubeQueryInfo cqi = new CubeQueryInfo();
        cqi.cubeAST = ast;
        if (ast.getParent() != null) {
          ASTNode parent = (ASTNode) ast.getParent();
          if (isCubeKeywordNode((ASTNode) ast.getChild(0))) {
            cqi.startPos = ast.getCharPositionInLine();
          } else {
            ASTNode selectAST = (ASTNode) ast.getChild(1).getChild(1);
            // Left most child of select AST will have char position after select with
            // no selects in between. search for select backward from there.
            cqi.startPos = getStartPos(originalQuery, HQLParser.leftMostChild(selectAST).getCharPositionInLine(),
              "select");
          }
          int ci = ast.getChildIndex();
          if (parent.getToken() == null || parent.getType() == HiveParser.TOK_EXPLAIN
            || parent.getType() == HiveParser.TOK_CREATETABLE) {
            // Not a sub query
            cqi.endPos = originalQuery.length();
          } else if (parent.getChildCount() > ci + 1
            || (parent.getParent() != null && parent.getType() == parent.getParent().getType())) {
            if (parent.getType() == HiveParser.TOK_SUBQUERY) {
              // less for the next start and for close parenthesis
              cqi.endPos = getEndPos(originalQuery, parent.getChild(ci + 1).getCharPositionInLine(), ")");
            } else if (parent.getType() == HiveParser.TOK_UNIONALL) {
              ASTNode nextChild;
              if (parent.getChildCount() > ci + 1) {
                // top level child
                nextChild = (ASTNode) parent.getChild(ci + 1);
              } else {
                // middle child, it's left child's right child.
                nextChild = (ASTNode) parent.getParent().getChild(parent.getChildIndex()+1);
              }
              // Go back one select
              cqi.endPos = getStartPos(originalQuery, nextChild.getChild(1).getChild(1).getCharPositionInLine() - 1,
                "select");
              cqi.endPos = getEndPos(originalQuery, cqi.endPos, "union all");
            } else {
              // Not expected to reach here
              log.warn("Unknown query pattern found with AST:{}", ast.dump());
              throw new LensException("Unknown query pattern");
            }
          } else {
            // last child of union all query
            // one for next AST
            // and one for the close parenthesis if there are no more unionall
            // or one for the string 'UNION ALL' if there are more union all
            log.debug("Child of union all");
            cqi.endPos = parent.getParent().getChild(1).getCharPositionInLine();
            if (cqi.endPos != 0) {
              cqi.endPos = getEndPos(originalQuery, cqi.endPos, ")", "UNION ALL");
            } else {
              cqi.endPos = originalQuery.length();
            }
          }
        }
        if (log.isDebugEnabled()) {
          log.debug("Adding cqi {} query:{}", cqi, originalQuery.substring(cqi.startPos, cqi.endPos));
        }
        cubeQueries.add(cqi);
      } else {
        for (int childPos = 0; childPos < childCount; ++childPos) {
          findCubePositions((ASTNode) ast.getChild(childPos), cubeQueries, originalQuery, conf);
        }
      }
    } else {
      log.warn("Null AST!");
    }
  }

  private static boolean isCubeTableNode(ASTNode node, HiveConf conf) throws LensException {
    if (node.getType() == HiveParser.TOK_TABREF || node.getType() == HiveParser.TOK_TABNAME) {
      return isCubeTableNode((ASTNode) node.getChild(0), conf);
    }
    if (node.getText().contains("JOIN")) {
      if (isCubeTableNode((ASTNode) node.getChild(0), conf)) {
        for (int i = 1; i < node.getChildCount(); i += 2) {
          if (!isCubeTableNode((ASTNode) node.getChild(i), conf)) {
            return false;
          }
        }
        return true;
      }
    }
    return node.getType() == HiveParser.Identifier && getClient(conf).isLensQueryableTable(node.getText());
  }

  private static boolean isFromNodeWithCubeTable(ASTNode child, HiveConf conf) throws LensException {
    return child.getType() == HiveParser.TOK_FROM && isCubeTableNode((ASTNode) child.getChild(0), conf);
  }

  public static CubeMetastoreClient getClient(HiveConf conf) throws LensException {
    try {
      return CubeMetastoreClient.getInstance(conf);
    } catch (HiveException e) {
      throw new LensException("Couldn't get instance of metastore client", e);
    }
  }

  private static boolean isCubeKeywordNode(ASTNode child) {
    return child.getType() == HiveParser.KW_CUBE;
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
    backTrackIndex = backTrack(query, backTrackIndex, false, backTrackStr);
    while (backTrackIndex > 0 && Character.isSpaceChar(query.charAt(backTrackIndex - 1))) {
      backTrackIndex--;
    }
    return backTrackIndex;
  }

  private static int backTrack(String query, int backTrackIndex, boolean force, String... backTrackStr) {
    if (backTrackStr != null) {
      String q = query.substring(0, backTrackIndex).toLowerCase();
      String qTrim = q.trim();
      for (String aBackTrackStr : backTrackStr) {
        if ((force  && qTrim.contains(aBackTrackStr.toLowerCase()))|| qTrim.endsWith(aBackTrackStr.toLowerCase())) {
          backTrackIndex = q.lastIndexOf(aBackTrackStr.toLowerCase());
          break;
        }
      }
    }
    return backTrackIndex;
  }

  /**
   * Gets the end pos.
   *
   * @param query          the query
   * @param backTrackIndex the back track index
   * @param backTrackStr   the back track str
   * @return the end pos
   */
  private static int getStartPos(String query, int backTrackIndex, String... backTrackStr) {
    backTrackIndex = backTrack(query, backTrackIndex, true, backTrackStr);
    while (backTrackIndex < query.length() && Character.isSpaceChar(query.charAt(backTrackIndex))) {
      backTrackIndex++;
    }
    return backTrackIndex;
  }

  /**
   * Gets the rewriter.
   *
   * @param queryConf the query conf
   * @return the rewriter
   * @throws LensException the lens exception
   */
  static CubeQueryRewriter getCubeRewriter(Configuration queryConf, HiveConf hconf) throws LensException {
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

      String replacedQuery = getReplacedQuery(ctx.getPhase1RewrittenQuery());
      Map<LensDriver, DriverRewriterRunnable> runnables = new LinkedHashMap<>();
      List<RewriteUtil.CubeQueryInfo> cubeQueries = findCubePositions(replacedQuery, ctx.getHiveConf());

      for (LensDriver driver : ctx.getDriverContext().getEligibleDrivers()) {
        runnables.put(driver, new DriverRewriterRunnable(driver, ctx, cubeQueries, replacedQuery));
      }

      return runnables;
    } catch (LensException e) {

      /* REST layer has to catch LensException and add additional information to it.
      To enable REST layer to call methods on LensException instance, we have to propagate
      LensException to the REST layer. Since there is a Exception e catch here,
      we need to explicitly catch and rethrow LensException to enable us for the same.*/

      throw e;
    } catch (Exception e) {
      throw new LensException("Rewriting failed, cause :" + e.getMessage(), e);
    }
  }

  public static DriverQueryPlan getRewriterPlan(DriverRewriterRunnable rewriter) {
    return new RewriterPlan(rewriter.cubeQueryCtx);
  }

  public static class DriverRewriterRunnable implements Runnable {
    @Getter
    private final LensDriver driver;
    private final AbstractQueryContext ctx;
    private final List<CubeQueryInfo> cubeQueries;
    private final String replacedQuery;
    /** Cube query context - set after rewriting */
    private List<CubeQueryContext> cubeQueryCtx;

    @Getter
    /** Indicate if rewrite operation succeeded */
    private boolean succeeded;

    @Getter
    /** Get cause of rewrite failure if rewrite operation failed */
    private String failureCause = null;

    @Getter
    private LensException cause;

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
      if (cubeQueries != null) {
        cubeQueryCtx = new ArrayList<>(cubeQueries.size());
      }
    }

    @Override
    public void run() {
      String lowerCaseQuery = replacedQuery.toLowerCase();
      if (lowerCaseQuery.startsWith("add") || lowerCaseQuery.startsWith("set")) {
        rewrittenQuery = replacedQuery;
        return;
      }

      MethodMetricsContext rewriteGauge = MethodMetricsFactory
        .createMethodGauge(ctx.getDriverConf(driver), true, REWRITE_QUERY_GAUGE);
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
          log.debug("Rewriting cube query: {}", cqi.query);

          if (start != cqi.startPos) {
            builder.append(replacedQuery.substring(start, cqi.startPos));
          }

          // Parse and rewrite individual cube query
          CubeQueryContext cqc = rewriter.rewrite(cqi.query);
          MethodMetricsContext toHQLGauge = MethodMetricsFactory
            .createMethodGauge(ctx.getDriverConf(driver), true, qIndex + "-" + TOHQL_GAUGE);
          // toHQL actually generates the rewritten query
          String hqlQuery = cqc.toHQL();
          cubeQueryCtx.add(cqc);
          toHQLGauge.markSuccess();
          qIndex++;

          log.debug("Rewritten query:{}", hqlQuery);

          builder.append(hqlQuery);
          start = cqi.endPos;
        }

        builder.append(replacedQuery.substring(start));

        rewrittenQuery = builder.toString();
        // set rewriter plan
        ctx.getDriverContext().setDriverRewriterPlan(driver, getRewriterPlan(this));
        succeeded = true;
        ctx.setDriverQuery(driver, rewrittenQuery);
        log.info("Final rewritten query for driver: {} is: {}", driver, rewrittenQuery);

      } catch (final LensException e) {

        this.cause = e;
        captureExceptionInformation(e);
      } catch (Exception e) {

        // we are catching all exceptions sothat other drivers can be picked in case of driver bugs
        captureExceptionInformation(e);
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

    private void captureExceptionInformation(final Exception e) {

      log.warn("Driver : {}  Skipped for the query rewriting due to ", driver, e);
      ctx.setDriverRewriteError(driver, e);
      failureCause = new StringBuilder(" Driver :")
        .append(driver.getFullyQualifiedName())
        .append(" Cause :" + e.getLocalizedMessage())
        .toString();
    }
  }
}
