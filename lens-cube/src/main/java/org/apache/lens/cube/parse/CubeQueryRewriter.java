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

import static org.apache.lens.cube.error.LensCubeErrorCode.SYNTAX_ERROR;

import java.io.IOException;
import java.util.List;

import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.metrics.MethodMetricsContext;
import org.apache.lens.server.api.metrics.MethodMetricsFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Rewrites given cube query into simple storage table HQL.
 */
@Slf4j
public class CubeQueryRewriter {
  private final Configuration conf;
  @VisibleForTesting
  @Getter(AccessLevel.PACKAGE)
  private final ImmutableList<ContextRewriter> rewriters;
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
    ImmutableList.Builder<ContextRewriter> builder = ImmutableList.builder();
    setupRewriters(builder);
    rewriters = builder.build();
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
   * MaxCoveringFactResolver runs just after all candidate facts' partitions
   * are resolved. It then sees how much time range each fact set is able to cover
   * and finds the maximum coverable range. It then prunes all fact sets that
   * are covering less than that range. If fail on partial is true, then by the
   * time this resolver runs, all the candidate fact sets cover full range.
   * So there this resolver is a no-op. Same thing when fail on partial is false
   * and no fact set has any data. This is most useful when facts actually have
   * partial data. There it'll ensure the facts that are covering the maximum
   * time range will be picked.
   *
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
  private void setupRewriters(ImmutableList.Builder<ContextRewriter> rewriters) {
    // Resolve columns - the column alias and table alias
    rewriters.add(new ColumnResolver());
    // Rewrite base trees (groupby, having, orderby, limit) using aliases
    rewriters.add(new AliasReplacer());
    ExpressionResolver exprResolver = new ExpressionResolver();
    DenormalizationResolver denormResolver = new DenormalizationResolver();
    CandidateTableResolver candidateTblResolver = new CandidateTableResolver();
    StorageTableResolver storageTableResolver = new StorageTableResolver(conf);
    LightestFactResolver lightestFactResolver = new LightestFactResolver();

    // Phase 1 of exprResolver: Resolve expressions
    rewriters.add(exprResolver);
    // Phase 1 of denormResolver: De-normalized columns resolved
    rewriters.add(denormResolver);
    // Resolve time ranges
    rewriters.add(new TimerangeResolver());
    // Phase 1 of candidateTblResolver: Resolve candidate storages and dimension tables for columns queried
    rewriters.add(candidateTblResolver);
    // Resolve aggregations and generate base select tree
    rewriters.add(new AggregateResolver());
    rewriters.add(new GroupbyResolver(conf));
    //validate fields queryability (in case of derived cubes setup)
    rewriters.add(new FieldValidator());
    // Resolve joins and generate base join tree
    rewriters.add(new JoinResolver());
    // Do col life validation for the time range(s) queried
    rewriters.add(new ColumnLifetimeChecker());
    // Phase 1 of storageTableResolver: Validate and prune candidate storages
    rewriters.add(storageTableResolver);
    // Phase 2 of candidateTblResolver: Resolve candidate storages and dimension tables for columns included
    // in join and denorm resolvers
    rewriters.add(candidateTblResolver);
    // Find Union and Join combinations over Storage Candidates that can answer the queried time range(s) and all
    // queried measures
    rewriters.add(new CandidateCoveringSetsResolver());

    // If lightest fact first option is enabled for this driver (via lens.cube.query.pick.lightest.fact.first = true),
    // run LightestFactResolver and keep only the lighted combination(s) generated by CandidateCoveringSetsResolver
    if (lightFactFirst) {
      // Prune candidate tables for which denorm column references do not exist
      rewriters.add(denormResolver);
      // Phase 2 of exprResolver:Prune candidate facts without any valid expressions
      rewriters.add(exprResolver);
      // Pick the least cost combination(s) (and prune others) out of a set of combinations produced
      // by CandidateCoveringSetsResolver
      rewriters.add(lightestFactResolver);
    }

    // Phase 2 of storageTableResolver: resolve storage table partitions.
    rewriters.add(storageTableResolver);
    // For all segmentation candidates, for all segments, modify query ast and perform rewrites on inner cubes
    // Also takes care of performing rewrites in segmentation candidates under a union/join candidate
    rewriters.add(new SegmentationInnerRewriter(conf, hconf));
    // In case partial data is allowed (via lens.cube.query.fail.if.data.partial = false) and there are many
    // combinations with partial data, pick the one that covers the maximum part of time ranges(s) queried
    rewriters.add(new MaxCoveringFactResolver(conf));
    // Phase 3 of storageTableResolver:  resolve dimension tables and partitions.
    rewriters.add(storageTableResolver);

    //TODO union: phase 2 of denormResolver needs to be moved before CoveringSetResolver.. check if this makes sense

    // Prune candidate tables for which denorm column references do not exist
    rewriters.add(denormResolver);
    // Phase 2 of exprResolver : Prune candidate facts without any valid expressions
    rewriters.add(exprResolver);

    // Pick the least cost combination(s) (and prune others) out of a set of combinations produced
    // by CandidateCoveringSetsResolver
    rewriters.add(lightestFactResolver);

    // if two combinations have the same least weight/cost, then the combination with least number of time partitions
    // queried will be picked. Rest of the combinations will be pruned
    rewriters.add(new LeastPartitionResolver());
    rewriters.add(new LightestDimensionResolver());
    // Takes all candidates remaining till now, tries to explode that.
    // see CandidateExploder#rewriteContext and Candidate#explode for further documentation
    rewriters.add(new CandidateExploder());
  }

  public CubeQueryContext rewrite(ASTNode astnode) throws LensException {
    CubeSemanticAnalyzer analyzer;
    try {
      analyzer = new CubeSemanticAnalyzer(conf, hconf);
      analyzer.analyze(astnode, qlCtx);
    } catch (SemanticException e) {
      throw new LensException(SYNTAX_ERROR.getLensErrorInfo(), e, e.getMessage());
    }
    CubeQueryContext ctx = new CubeQueryContext(astnode, analyzer.getCubeQB(), conf, hconf);
    rewrite(rewriters, ctx);
    return ctx;
  }

  public CubeQueryContext rewrite(String command) throws LensException {
    if (command != null) {
      command = command.replace("\n", "");
    }
    ASTNode tree;
    try {
      ParseDriver pd = new ParseDriver();
      tree = pd.parse(command, qlCtx, false);
      tree = ParseUtils.findRootNonNullToken(tree);
    } catch (ParseException e) {
      throw new LensException(SYNTAX_ERROR.getLensErrorInfo(), e, e.getMessage());
    }
    return rewrite(tree);
  }

  private static final String ITER_STR = "-ITER-";

  private void rewrite(List<ContextRewriter> rewriters, CubeQueryContext ctx) throws LensException {
    int i = 0;
    for (ContextRewriter rewriter : rewriters) {
      /*
       * Adding iteration number as part of gauge name since some rewriters have more than one phase, and having
       * iter number gives the idea which iteration the rewriter was run
       */
      MethodMetricsContext mgauge = MethodMetricsFactory.createMethodGauge(ctx.getConf(), true,
        rewriter.getClass().getCanonicalName() + ITER_STR + i);

      rewriter.rewriteContext(ctx);
      mgauge.markSuccess();
      i++;
    }
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
