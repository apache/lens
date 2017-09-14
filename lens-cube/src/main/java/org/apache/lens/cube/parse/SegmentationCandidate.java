/*
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

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

import static org.apache.lens.cube.metadata.DateUtil.formatAbsDate;
import static org.apache.lens.cube.metadata.MetastoreUtil.getStringLiteralAST;

import static org.apache.hadoop.hive.ql.parse.HiveParser.Identifier;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_FROM;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_HAVING;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_INSERT;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_ORDERBY;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_SELEXPR;

import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.lens.cube.metadata.Cube;
import org.apache.lens.cube.metadata.CubeColumn;
import org.apache.lens.cube.metadata.CubeInterface;
import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.metadata.MetastoreUtil;
import org.apache.lens.cube.metadata.Segment;
import org.apache.lens.cube.metadata.Segmentation;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ASTNode;

import org.antlr.runtime.CommonToken;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.Getter;

/**
 * Created on 09/03/17.
 */
public class SegmentationCandidate implements Candidate {

  Collection<String> columns;
  @Getter
  private final CubeQueryContext cubeQueryContext;
  private Segmentation segmentation;
  private Map<String, Cube> cubesOfSegmentation;
  Map<String, CubeQueryContext> cubeQueryContextMap;
  @Getter
  private final Set<Integer> answerableMeasurePhraseIndices = Sets.newHashSet();
  private Map<TimeRange, TimeRange> queriedRangeToMyRange = Maps.newHashMap();

  SegmentationCandidate(CubeQueryContext cubeQueryContext, Segmentation segmentation) throws LensException {
    this.cubeQueryContext = cubeQueryContext;
    this.segmentation = segmentation;
    cubesOfSegmentation = Maps.newHashMap();
    cubeQueryContextMap = Maps.newHashMap();
    for (Segment segment : segmentation.getSegments()) {
      // assuming only base cubes in segmentation
      cubesOfSegmentation.put(segment.getName(), (Cube) getCubeMetastoreClient().getCube(segment.getName()));
    }
  }


  public SegmentationCandidate explode() throws LensException {
    return this;
  }

  private static <T> Predicate<T> not(Predicate<T> predicate) {
    return predicate.negate();
  }

  boolean rewriteInternal(Configuration conf, HiveConf hconf) throws LensException {
    CubeInterface cube = getCube();
    if (cube == null) {
      return false;
    }
    for (Segment segment : segmentation.getSegments()) {
      // assuming only base cubes in segmentation
      Cube innerCube = (Cube) getCubeMetastoreClient().getCube(segment.getName());
      cubesOfSegmentation.put(segment.getName(), innerCube);
      Set<QueriedPhraseContext> notAnswerable = cubeQueryContext.getQueriedPhrases().stream()
        .filter(not(this::isPhraseAnswerable)).collect(Collectors.toSet());
      // create ast
      ASTNode ast = MetastoreUtil.copyAST(cubeQueryContext.getAst(),
        astNode -> {
          // replace time range
          for (Map.Entry<TimeRange, TimeRange> timeRangeTimeRangeEntry : queriedRangeToMyRange.entrySet()) {
            TimeRange queriedTimeRange = timeRangeTimeRangeEntry.getKey();
            TimeRange timeRange = timeRangeTimeRangeEntry.getValue();
            if (astNode.getParent() == queriedTimeRange.getAstNode()) {
              if (astNode.getChildIndex() == 2) {
                return Pair.of(getStringLiteralAST(formatAbsDate(timeRange.getFromDate())), false);
              } else if (astNode.getChildIndex() == 3) {
                return Pair.of(getStringLiteralAST(formatAbsDate(timeRange.getToDate())), false);
              }
              break;
            }
          }
          // else, replace unanswerable measures
          for (QueriedPhraseContext phraseContext : notAnswerable) {
            if ((astNode.getType() != TOK_SELEXPR && astNode == phraseContext.getExprAST())
              || astNode.getParent() == phraseContext.getExprAST()) {
              return Pair.of(MetastoreUtil.copyAST(UnionQueryWriter.DEFAULT_MEASURE_AST), false);
            }
          }
          // else, copy token replacing cube name and ask for recursion on child nodes
          // this is hard copy. Default is soft copy, which is new ASTNode(astNode)
          // Soft copy retains the token object inside it, hard copy copies token object
          return Pair.of(new ASTNode(new CommonToken(astNode.getToken())), true);
        });
      addCubeNameAndAlias(ast, innerCube);
      trimHavingAndOrderby(ast, innerCube);
      Configuration innerConf = conf;
      if (conf.get(LensConfConstants.QUERY_METRIC_UNIQUE_ID_CONF_KEY) != null) {
        innerConf = new Configuration(conf);
        innerConf.set(LensConfConstants.QUERY_METRIC_UNIQUE_ID_CONF_KEY,
          conf.get(LensConfConstants.QUERY_METRIC_UNIQUE_ID_CONF_KEY) + "-" + segment.getName());
      }
      CubeQueryRewriter rewriter = new CubeQueryRewriter(innerConf, hconf);
      CubeQueryContext ctx = rewriter.rewrite(ast);
      cubeQueryContextMap.put(segment.getName(), ctx);
      if (!ctx.getCandidates().isEmpty()) {
        ctx.pickCandidateToQuery();
        for (StorageCandidate storageCandidate : CandidateUtil.getStorageCandidates(ctx.getPickedCandidate())) {
          for (Map.Entry<TimeRange, TimeRange> timeRangeTimeRangeEntry : queriedRangeToMyRange.entrySet()) {
            TimeRange timeRange = timeRangeTimeRangeEntry.getKey();
            TimeRange queriedTimeRange = timeRangeTimeRangeEntry.getValue();
            Set<FactPartition> rangeToPartition = storageCandidate.getRangeToPartitions().get(timeRange);
            if (rangeToPartition != null) {
              storageCandidate.getRangeToPartitions().put(queriedTimeRange, rangeToPartition);
            }
            String extraWhere = storageCandidate.getRangeToExtraWhereFallBack().get(timeRange);
            if (extraWhere != null) {
              storageCandidate.getRangeToExtraWhereFallBack().put(queriedTimeRange, extraWhere);
            }
          }
        }
      }
    }
    return areCandidatesPicked();
  }

  private void addCubeNameAndAlias(ASTNode ast, Cube innerCube) {
    ASTNode cubeNameNode = findCubeNameNode(HQLParser.findNodeByPath(ast, TOK_FROM));
    assert cubeNameNode != null;
    ASTNode tabrefNode = (ASTNode) cubeNameNode.getParent().getParent();
    cubeNameNode.getToken().setText(innerCube.getName());
    ASTNode aliasNode = new ASTNode(new CommonToken(Identifier,
      getCubeQueryContext().getAliasForTableName(getCube().getName())));
    if (tabrefNode.getChildCount() > 1) {
      tabrefNode.setChild(1, aliasNode);
    } else {
      tabrefNode.addChild(aliasNode);
    }
  }

  private ASTNode findCubeNameNode(ASTNode node) {
    if (node.getType() == Identifier) {
      if (node.getText().equalsIgnoreCase(getCubeQueryContext().getCube().getName())) {
        return node;
      } else {
        return null; // should never come here.
      }
    }
    return node.getChildren().stream().map(ASTNode.class::cast).map(this::findCubeNameNode).filter(Objects::nonNull)
      .findFirst().orElse(null);
  }

  private void trimHavingAndOrderby(ASTNode ast, Cube innerCube) {
    ASTNode havingAst = HQLParser.findNodeByPath(ast, TOK_INSERT, TOK_HAVING);
    if (havingAst != null) {
      ASTNode newHavingAst = HQLParser.trimHavingAst(havingAst, innerCube.getAllFieldNames());
      if (newHavingAst != null) {
        havingAst.getParent().setChild(havingAst.getChildIndex(), newHavingAst);
      } else {
        havingAst.getParent().deleteChild(havingAst.getChildIndex());
      }
    }
    ASTNode orderByAst = HQLParser.findNodeByPath(ast, TOK_INSERT, TOK_ORDERBY);
    if (orderByAst != null) {
      ASTNode newOrderByAst = HQLParser.trimOrderByAst(orderByAst, innerCube.getAllFieldNames());
      if (newOrderByAst != null) {
        orderByAst.getParent().setChild(orderByAst.getChildIndex(), newOrderByAst);
      } else {
        orderByAst.getParent().deleteChild(orderByAst.getChildIndex());
      }
    }
  }


  public SegmentationCandidate(SegmentationCandidate segmentationCandidate) throws LensException {
    this(segmentationCandidate.cubeQueryContext, segmentationCandidate.segmentation);

  }

  @Override
  public Collection<String> getColumns() {
    if (columns == null) {
      columns = cubeStream().map(Cube::getAllFieldNames)
        .reduce(Sets::intersection).orElseGet(Sets::newHashSet)
        .stream().collect(Collectors.toSet());
    }
    return columns;
  }

  @Override
  public Date getStartTime() {
    return segmentation.getStartTime();
  }

  @Override
  public Date getEndTime() {
    return segmentation.getEndTime();
  }

  @Override
  public OptionalDouble getCost() {
    if (areCandidatesPicked()) {
      double cost = 0.0;
      for (Candidate candidate : getChildren()) {
        if (candidate.getCost().isPresent()) {
          cost += candidate.getCost().getAsDouble();
        } else {
          return OptionalDouble.empty();
        }
      }
      return OptionalDouble.of(cost);
    } else {
      return OptionalDouble.empty();
    }
  }

  @Override
  public boolean contains(Candidate candidate) {
    return areCandidatesPicked() && getChildren().contains(candidate);
  }

  @Override
  public Collection<Candidate> getChildren() {
    return candidateStream().collect(Collectors.toSet());
  }

  @Override
  public int getChildrenCount() {
    return segmentation.getSegments().size();
  }

  @Override
  public boolean isTimeRangeCoverable(TimeRange timeRange) throws LensException {
    return true;
  }

  @Override
  public boolean evaluateCompleteness(TimeRange timeRange, TimeRange queriedTimeRange, boolean failOnPartialData)
    throws LensException {
    queriedRangeToMyRange.put(queriedTimeRange, timeRange);
    return true;
  }

  @Override
  public Set<FactPartition> getParticipatingPartitions() {
    Set<FactPartition> partitionSet = Sets.newHashSet();
    for (CubeQueryContext cubeQueryContext : cubeQueryContextMap.values()) {
      if (cubeQueryContext.getPickedCandidate() != null) {
        partitionSet.addAll(cubeQueryContext.getPickedCandidate().getParticipatingPartitions());
      }
    }
    return partitionSet;
  }

  @Override
  public boolean isExpressionEvaluable(ExpressionResolver.ExpressionContext expr) {
    // expression context is specific to cubequerycontext. So for segmentation candidate,
    // I can't ask my children to check this context for evaluability.
    return cubeStream()
      .map(cube -> cube.getExpressionByName(expr.getExprCol().getName()))
      .allMatch(Objects::nonNull);
  }

  private boolean areCandidatesPicked() {
    return candidateStream().count() == cubesOfSegmentation.size();
  }

  private Stream<Candidate> candidateStream() {
    return contextStream().map(CubeQueryContext::getPickedCandidate).filter(Objects::nonNull);
  }

  private Stream<CubeQueryContext> contextStream() {
    return cubeQueryContextMap.values().stream();
  }

  private Stream<Cube> cubeStream() {
    return cubesOfSegmentation.values().stream();
  }

  @Override
  public boolean isExpressionEvaluable(String expr) {
    return candidateStream().allMatch(cand -> cand.isExpressionEvaluable(expr));
  }

  @Override
  public boolean isDimAttributeEvaluable(String dim) throws LensException {
    if (areCandidatesPicked()) {
      for (Candidate childCandidate : (Iterable<Candidate>) candidateStream()::iterator) {
        if (!childCandidate.isDimAttributeEvaluable(dim)) {
          return false;
        }
      }
      return true;
    }
    return hasColumn(dim);
  }

  @Override
  public Candidate copy() throws LensException {
    return new SegmentationCandidate(this);
  }

  @Override
  public boolean isPhraseAnswerable(QueriedPhraseContext phrase) {
    // TODO consider measure start time etc
    return getColumns().containsAll(phrase.getColumns());
  }

  @Override
  public Optional<Date> getColumnStartTime(String column) {
    if (areCandidatesPicked()) {
      return candidateStream()
        .map(c -> c.getColumnStartTime(column))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .min(Comparator.naturalOrder());
    } else {
      return cubeStream()
        .map(cube -> cube.getColumnByName(column))
        .map(CubeColumn::getStartTime).filter(Objects::nonNull)
        .min(Comparator.naturalOrder());
    }
  }

  @Override
  public Optional<Date> getColumnEndTime(String column) {
    if (areCandidatesPicked()) {
      return candidateStream()
        .map(c -> c.getColumnEndTime(column))
        .filter(Optional::isPresent) // use flatmap(Optional::stream) after migration to java9
        .map(Optional::get)          // https://bugs.openjdk.java.net/browse/JDK-8050820
        .max(Comparator.naturalOrder());
    } else {
      return cubeStream()
        .map(cube -> cube.getColumnByName(column))
        .map(CubeColumn::getEndTime).filter(Objects::nonNull)
        .max(Comparator.naturalOrder());
    }
  }

  public void addAnswerableMeasurePhraseIndices(int index) {
    answerableMeasurePhraseIndices.add(index);
  }


  public String toString() {
    Collector<CharSequence, ?, String> collector = joining("; ", "SEG[", "]");
    if (areCandidatesPicked()) {
      return candidateStream().map(Candidate::toString).collect(collector);
    } else {
      return cubeStream().map(Cube::getName).collect(collector);
    }
  }

  Map<String, PruneCauses<Candidate>> getPruneCausesOfFailedContexts() {
    return cubeQueryContextMap.entrySet().stream().filter(entry -> entry.getValue().getPickedCandidate() == null)
      .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().getStoragePruningMsgs()));
  }

  @Override
  public boolean equals(Object obj) {
    if (super.equals(obj)) {
      return true;
    }

    if (obj == null || !(obj instanceof SegmentationCandidate)) {
      return false;
    }

    SegmentationCandidate segmantationCandidate = (SegmentationCandidate) obj;
    return (segmantationCandidate.segmentation.getSegments().equals(this.segmentation.getSegments())
        && segmantationCandidate.segmentation.getBaseCube().equals(this.segmentation.getBaseCube()));
  }

  @Override
  public int hashCode() {
    return segmentation.hashCode();
  }

}
