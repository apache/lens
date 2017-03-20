package org.apache.lens.cube.parse;

import static java.util.stream.Collectors.joining;
import static org.apache.hadoop.hive.ql.parse.HiveParser.Identifier;
import static org.apache.hadoop.hive.ql.parse.HiveParser.StringLiteral;
import static org.apache.lens.cube.metadata.DateUtil.formatAbsDate;

import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.lens.api.ds.Tuple2;
import org.apache.lens.cube.metadata.Cube;
import org.apache.lens.cube.metadata.CubeColumn;
import org.apache.lens.cube.metadata.CubeInterface;
import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.metadata.MetastoreUtil;
import org.apache.lens.cube.metadata.Segment;
import org.apache.lens.cube.metadata.Segmentation;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.error.LensException;

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
  private final Configuration conf;
  private final HiveConf hconf;
  private Map<String, Cube> cubesOfSegmentation;
  Map<String, CubeQueryContext> cubeQueryContextMap;
  @Getter
  private final Set<Integer> answerableMeasurePhraseIndices = Sets.newHashSet();
  private static final String IGNORE_KEY = "segmentations.to.ignore";

  public SegmentationCandidate(CubeQueryContext cubeQueryContext, Segmentation segmentation, Configuration conf, HiveConf hconf) throws LensException {
    this.cubeQueryContext = cubeQueryContext;
    this.segmentation = segmentation;
    this.conf = new Configuration(conf);
    this.hconf = hconf;
    cubesOfSegmentation = Maps.newHashMap();
    cubeQueryContextMap = Maps.newHashMap();
    for (Segment segment : segmentation.getSegments()) {
      // assuming only base cubes in segmentation
      cubesOfSegmentation.put(segment.getName(), (Cube) cubeQueryContext.getMetastoreClient().getCube(segment.getName()));
    }
  }

  public void explode(TimeRange timeRange, TimeRange queriedTimeRange) throws LensException {
    Collection<String> toIgnore = conf.getStringCollection(IGNORE_KEY); //TODO remove this, this is only a hack to avoid infinite loop and stack overflow
    if (toIgnore.contains(segmentation.getName())) {
      throw new LensException("Segmentation to be ignored");
    } else {
      toIgnore.add(segmentation.getName());
      conf.setStrings(IGNORE_KEY, toIgnore.toArray(new String[toIgnore.size()]));
    }
    CubeInterface cube = cubeQueryContext.getCube();
    for (Segment segment : segmentation.getSegments()) {
      // assuming only base cubes in segmentation
      cubesOfSegmentation.put(segment.getName(), (Cube) cubeQueryContext.getMetastoreClient().getCube(segment.getName()));
      Set<QueriedPhraseContext> notAnswerable = cubeQueryContext.getQueriedPhrases().stream().filter((phrase) -> !isPhraseAnswerable(phrase)).collect(Collectors.toSet());
      // create ast
      // todo: drop order by, having, limit
      ASTNode ast = MetastoreUtil.copyAST(cubeQueryContext.getAst(),
        astNode -> {
          // replace time range
          if (astNode.getParent() == timeRange.getAstNode()) {
            if (astNode.getChildIndex() == 2) {
              return Tuple2.of(new ASTNode(new CommonToken(StringLiteral, formatAbsDate(timeRange.getFromDate()))), false);
            } else if (astNode.getChildIndex() == 3) {
              return Tuple2.of(new ASTNode(new CommonToken(StringLiteral, formatAbsDate(timeRange.getToDate()))), false);
            }
          }
          // else, replace unanswerable measures
          for (QueriedPhraseContext phraseContext : notAnswerable) {
            if (astNode.getParent() == phraseContext.getExprAST()) {
              return Tuple2.of(MetastoreUtil.copyAST(UnionQueryWriter.DEFAULT_MEASURE_AST), false);
            }
          }
          // else, copy token replacing cube name and ask for recursion on child nodes
          // this is hard copy. Default is soft copy, which is new ASTNode(astNode)
          // Soft copy retains the token object inside it, hard copy copies token object
          ASTNode copy = new ASTNode(new CommonToken(astNode.getToken()));
          if (copy.getType() == Identifier) {
            copy.getToken().setText(copy.getToken().getText().replaceAll("(?i)"+cube.getName(), segment.getName()));
          }
          return Tuple2.of(copy, true);
        });
      // TODO modify time ranges. Nothing for now
      CubeQueryRewriter rewriter = new CubeQueryRewriter(conf, hconf);
      CubeQueryContext ctx = rewriter.rewrite(ast);
      // so that exception comes early
      // TODO: optimize
      ctx.toHQL();
      for (StorageCandidate storageCandidate : CandidateUtil.getStorageCandidates(ctx.getPickedCandidate())) {
        storageCandidate.getRangeToWhere().put(queriedTimeRange, storageCandidate.getRangeToWhere().get(timeRange));
      }
      cubeQueryContextMap.put(segment.getName(), ctx);
    }
  }

  public SegmentationCandidate(SegmentationCandidate segmentationCandidate) throws LensException {
    this(segmentationCandidate.cubeQueryContext, segmentationCandidate.segmentation,
      segmentationCandidate.conf, segmentationCandidate.hconf);
  }

  @Override
  public Collection<String> getColumns() {
    if (columns == null) {
      columns = cubeStream().map(Cube::getAllFields)
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
    if (!areCandidatesPicked()) {
      try {
        explode(timeRange, queriedTimeRange);
      } catch (LensException e) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Set<FactPartition> getParticipatingPartitions() {
    //TODO implement this
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
    if (areCandidatesPicked()) {
      return candidateStream().allMatch(cand -> cand.isExpressionEvaluable(expr));
    } else {
      return cubeStream()
        .map(cube -> cube.getExpressionByName(expr.getExprCol().getName()))
        .allMatch(Predicate.isEqual(expr.getExprCol()));
    }
  }

  public boolean areCandidatesPicked() {
    return candidateStream().count() == cubesOfSegmentation.size();
  }

  private Stream<Candidate> candidateStream() {
    return cubeQueryContextMap.values().stream().map(CubeQueryContext::getPickedCandidate);
  }

  private Stream<Cube> cubeStream() {
    return cubesOfSegmentation.values().stream();
  }

  @Override
  public boolean isExpressionEvaluable(String expr) {
    if (areCandidatesPicked()) {
      return candidateStream().allMatch(cand -> cand.isExpressionEvaluable(expr));
    }
    throw new IllegalAccessError("I don't know");
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
    throw new IllegalAccessError("I don't know");
  }

  @Override
  public Candidate copy() throws LensException {
    return new SegmentationCandidate(this);
  }

  @Override
  public boolean isPhraseAnswerable(QueriedPhraseContext phrase) {
    // TODO consider measure start time etc
    return getColumns().containsAll(phrase.getQueriedMsrs());
  }

  @Override
  public Optional<Date> getColumnStartTime(String column) {
    if (areCandidatesPicked()) {
      return candidateStream()
        .map(c -> c.getColumnStartTime(column))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .min(Comparator.naturalOrder()); // TODO recheck min
    } else {
      return cubeStream()
        .map(cube -> cube.getColumnByName(column))
        .map(CubeColumn::getStartTime).filter(Objects::nonNull)
        .min(Comparator.naturalOrder()); // todo recheck min
    }
  }

  @Override
  public Optional<Date> getColumnEndTime(String column) {
    if (areCandidatesPicked()) {
      return candidateStream()
        .map(c -> c.getColumnEndTime(column))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .max(Comparator.naturalOrder()); // TODO recheck max
    } else {
      return cubeStream()
        .map(cube -> cube.getColumnByName(column))
        .map(CubeColumn::getEndTime).filter(Objects::nonNull)
        .max(Comparator.naturalOrder()); // todo recheck max
    }
  }

  public void addAnswerableMeasurePhraseIndices(int index) {
    answerableMeasurePhraseIndices.add(index);
  }

  public String toString() {
    Collector<CharSequence, ?, String> collector = joining(", ", "SEG[", "]");
    if (areCandidatesPicked()) {
      return candidateStream().map(Candidate::toString).collect(collector);
    } else {
      return cubeStream().map(Cube::getName).collect(collector);
    }
  }
}
