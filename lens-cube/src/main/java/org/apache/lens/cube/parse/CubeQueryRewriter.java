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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lens.server.api.metrics.MethodMetricsContext;
import org.apache.lens.server.api.metrics.MethodMetricsFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.*;

import lombok.extern.slf4j.Slf4j;

/**
 * Rewrites given cube query into simple storage table HQL.
 */
@Slf4j
public class CubeQueryRewriter {
  private final Configuration conf;
  private final List<ContextRewriter> rewriters = new ArrayList<ContextRewriter>();
  private final HiveConf hconf;
  private Context qlCtx = null;
  private boolean lightFactFirst;

  public CubeQueryRewriter(Configuration conf, HiveConf hconf) {
    this.conf = conf;
    this.hconf = hconf;
    try {
      qlCtx = new Context(conf);
    } catch (IOException e) {
      // IOException is ignorable
    }
    lightFactFirst =
      conf.getBoolean(CubeQueryConfUtil.LIGHTEST_FACT_FIRST, CubeQueryConfUtil.DEFAULT_LIGHTEST_FACT_FIRST);
    setupRewriters();
  }

  /*
   * Here is the rewriter flow.
   *
   * Expression resolver: replaces the queried expression columns with their
   * expression ASTs in the query AST.
   *
   * ColumnResolver : ColumnResolver finds all the columns in the query AST
   *
   * AliasReplacer: - Finds queried column to table alias. - Finds queried dim
   * attributes and queried measures. - Does queried field validation wrt
   * derived cubes, if all fields of queried cube cannot be queried together. -
   * Replaces all the columns in all expressions with tablealias.column
   *
   * DenormalizationResolver: Phase 1: Finds all the queried column references
   * if any.
   *
   * CandidateTableResolver: Phase 1: - Prune candidate fact tables if queried
   * dim attributes are not present. - Also Figures out if queried column is not
   * part of candidate table, but it is a denormalized field which can reached
   * through a reference - Finds all the candidate fact sets containing queried
   * measures. Prunes facts which do not contain any of the queried measures.
   *
   * JoinResolver : - Finds all the join chains for between tables queried.
   *
   * TimerangeResolver : - Finds all timeranges in the query and does validation
   * wrt the queried field's life and the range queried
   *
   * CandidateTableResolver: Phase 2: - Prunes candidates tables if required
   * join columns are not part of candidate tables - Required source
   * columns(join columns) for reaching a denormalized field, are not part of
   * candidate tables - Required denormalized fields are not part of refered
   * tables, there by all the candidates which are using denormalized fields.
   *
   * AggregateResolver : - If non default aggregate or no aggregate queried for
   * a measure, it prunes all aggregate facts from candidates. - Replaces
   * measures with default aggregates. if enabled
   *
   * GroupbyResolver : - Promotes select to groupby and groupby to select, if
   * enabled
   *
   * StorageTableResolver : - Resolves storages and partitions for all candidate
   * tables. Prunes candidates if not storages are available.
   *
   * Whenever a candidate fact is pruned (because of no storages, no default
   * aggregate and etc), the sets containing the fact are also pruned.
   *
   * LeastPartitionResolver and LightestFactResolver work on candidate fact sets
   * and considers sets with least number of partitions required and lightest
   * facts respectively.
   *
   * If LightestFact first flag is enabled, LightestFactResolver is applied
   * before StorageTableResolver.
   *
   * Once all rewriters are done, finally picks up one of the available
   * candidate sets to answer the query, after all the resolvers are done. Once
   * the final candidate fact set is picked, if number of elements in the fact
   * set is one, the query written as with cube query ASTs. If the number of
   * fact sets is more, then query rewritten with MultifactHQLContext which
   * writes a join query with each fact query. A fact query contains only the
   * fields queried from that fact. The ASTs corresponding to the fact are AST
   * copied from original query and the expressions missing from this fact
   * removed.
   */
  private void setupRewriters() {
    // Rewrite base trees with expressions expanded
    rewriters.add(new ExpressionResolver(conf));
    // Resolve columns - the column alias and table alias
    rewriters.add(new ColumnResolver(conf));
    // Rewrite base trees (groupby, having, orderby, limit) using aliases
    rewriters.add(new AliasReplacer(conf));
    DenormalizationResolver denormResolver = new DenormalizationResolver(conf);
    CandidateTableResolver candidateTblResolver = new CandidateTableResolver(conf);
    StorageTableResolver storageTableResolver = new StorageTableResolver(conf);
    // De-normalized columns resolved
    rewriters.add(denormResolver);
    // Resolve candidate fact tables and dimension tables for columns queried
    rewriters.add(candidateTblResolver);
    // Resolve joins and generate base join tree
    rewriters.add(new JoinResolver(conf));
    // resolve time ranges and do col life validation
    rewriters.add(new TimerangeResolver(conf));
    // Resolve candidate fact tables and dimension tables for columns included
    // in join and denorm resolvers
    rewriters.add(candidateTblResolver);
    // Resolve aggregations and generate base select tree
    rewriters.add(new AggregateResolver(conf));
    rewriters.add(new GroupbyResolver(conf));
    // Phase 1: resolve fact tables.
    rewriters.add(storageTableResolver);
    if (lightFactFirst) {
      rewriters.add(new LightestFactResolver(conf));
    }
    // Phase 2: resolve fact table partitions.
    rewriters.add(storageTableResolver);
    if (!lightFactFirst) {
      rewriters.add(new LightestFactResolver(conf));
    }
    // Phase 3: resolve dimension tables and partitions.
    rewriters.add(storageTableResolver);
    // Check for candidate tables using de-normalized columns
    rewriters.add(denormResolver);
    rewriters.add(new LeastPartitionResolver(conf));
    if (!lightFactFirst) {
      rewriters.add(new LightestFactResolver(conf));
    }
    rewriters.add(new LightestDimensionResolver(conf));
  }

  public CubeQueryContext rewrite(ASTNode astnode) throws SemanticException {
    CubeSemanticAnalyzer analyzer = new CubeSemanticAnalyzer(conf, hconf);
    analyzer.analyze(astnode, qlCtx);
    CubeQueryContext ctx = analyzer.getQueryContext();
    rewrite(rewriters, ctx);
    return ctx;
  }

  public CubeQueryContext rewrite(String command) throws ParseException, SemanticException {
    if (command != null) {
      command = command.replace("\n", "");
    }
    ParseDriver pd = new ParseDriver();
    ASTNode tree = pd.parse(command, qlCtx, false);
    tree = ParseUtils.findRootNonNullToken(tree);
    return rewrite(tree);
  }

  private static final String ITER_STR = "-ITER-";

  private void rewrite(List<ContextRewriter> rewriters, CubeQueryContext ctx) throws SemanticException {
    int i = 0;
    for (ContextRewriter rewriter : rewriters) {
      /*
       * Adding iteration number as part of gauge name since some rewriters are have more than one phase, and having
       * iter number gives the idea which iteration the rewriter was run
       */
      MethodMetricsContext mgauge = MethodMetricsFactory.createMethodGauge(ctx.getConf(), true,
        rewriter.getClass().getCanonicalName() + ITER_STR + i);
      rewriter.rewriteContext(ctx);
      mgauge.markSuccess();
      i++;
    }
  }

  public Context getQLContext() {
    return qlCtx;
  }

  public void clear() {
    try {
      if (qlCtx != null) {
        qlCtx.clear();
      }
    } catch (IOException e) {
      log.info("Ignoring exception in clearing qlCtx:", e);
      // ignoring exception in clear
    }
  }
}
