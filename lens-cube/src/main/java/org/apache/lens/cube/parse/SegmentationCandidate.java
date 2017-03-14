package org.apache.lens.cube.parse;

import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lens.cube.metadata.Cube;
import org.apache.lens.cube.metadata.CubeColumn;
import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.metadata.MetastoreUtil;
import org.apache.lens.cube.metadata.Segment;
import org.apache.lens.cube.metadata.Segmentation;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import org.antlr.runtime.CommonToken;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.Getter;

/**
 * Created on 09/03/17.
 */
public class SegmentationCandidate implements Candidate {

  Collection<String> columns;
  private CubeQueryContext cubeql;
  private Segmentation segmentation;
  private final Configuration conf;
  private final HiveConf hconf;
  private Map<String, Cube> cubesOfSegmentation;
  Map<String, CubeQueryContext> cubeQueryContextMap;
  public SegmentationCandidate(CubeQueryContext cubeql, Segmentation segmentation, Configuration conf, HiveConf hconf) throws LensException {
    this.cubeql = cubeql;
    this.segmentation = segmentation;
    this.conf = conf;
    this.hconf = hconf;
    cubesOfSegmentation = Maps.newHashMap();
    cubeQueryContextMap = Maps.newHashMap();
    for (Segment segment : segmentation.getSegments()) {
      // assuming only base cubes in segmentation
      cubesOfSegmentation.put(segment.getName(), (Cube) cubeql.getMetastoreClient().getCube(segment.getName()));
      // create ast
      ASTNode ast = MetastoreUtil.copyAST(cubeql.getAst(),
        s-> s.replaceAll(cubeql.getCube().getName(), segment.getName()));
      // TODO modify time ranges. Nothing for now
      CubeQueryRewriter rewriter = new CubeQueryRewriter(conf, hconf);
      CubeQueryContext ctx = rewriter.rewrite(ast);
      // so that exception comes early
      // TODO: optimize
      ctx.toHQL();
      cubeQueryContextMap.put(segment.getName(), ctx);
    }
  }

  public SegmentationCandidate(SegmentationCandidate segmentationCandidate) throws LensException {
    this(segmentationCandidate.cubeql, segmentationCandidate.segmentation,
      segmentationCandidate.conf, segmentationCandidate.hconf);
  }

  @Override
  public Collection<String> getColumns() {
    if (columns == null) {
      columns = cubesOfSegmentation.values().stream().map(Cube::getAllFields)
        .reduce(Sets::intersection).orElseGet(Sets::newHashSet)
        .stream().map(CubeColumn::getName).collect(Collectors.toSet());
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
  public double getCost() {
    return segmentation.weight();
  }

  @Override
  public boolean contains(Candidate candidate) {
    // TODO implement this. Not required in MVP hence leaving it for now
    return false;
  }

  @Override
  public Collection<Candidate> getChildren() {
    return null;
  }

  @Override
  public boolean evaluateCompleteness(TimeRange timeRange, TimeRange queriedTimeRange, boolean failOnPartialData) throws LensException {
    //TODO implement this
    return true;
  }

  @Override
  public Set<FactPartition> getParticipatingPartitions() {
    //TODO implement this
    Set<FactPartition> partitionSet = Sets.newHashSet();
    for (CubeQueryContext cubeQueryContext : cubeQueryContextMap.values()) {
      partitionSet.addAll(cubeQueryContext.getPickedCandidate().getParticipatingPartitions());
    }
    return partitionSet;
  }

  @Override
  public boolean isExpressionEvaluable(ExpressionResolver.ExpressionContext expr) {
    return false;
  }

  @Override
  public Set<Integer> getAnswerableMeasurePhraseIndices() {
    return null;
  }

  @Override
  public Candidate copy() throws LensException {
    return new SegmentationCandidate(this);
  }
}
