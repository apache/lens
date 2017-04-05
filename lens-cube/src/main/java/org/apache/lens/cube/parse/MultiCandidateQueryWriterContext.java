package org.apache.lens.cube.parse;

import java.util.List;

import org.apache.lens.server.api.error.LensException;

import com.google.common.collect.Lists;
import lombok.Getter;

/**
 * Created on 31/03/17.
 */
public class MultiCandidateQueryWriterContext implements QueryWriterContext {
  @Getter
  private List<QueryWriterContext> children;
  @Getter
  private CubeQueryContext cubeQueryContext;

  public MultiCandidateQueryWriterContext(List<QueryWriterContext> children, CubeQueryContext cubeQueryContext) {
    this.children = children;
    this.cubeQueryContext = cubeQueryContext;
  }

  public void addAutoJoinDims() throws LensException {
    for (QueryWriterContext candidate : getChildren()) {
      candidate.addAutoJoinDims();
    }
  }

  public void addExpressionDims() throws LensException {
    for (QueryWriterContext candidate : getChildren()) {
      candidate.addExpressionDims();
    }
  }

  public void addDenormDims() throws LensException {
    for (QueryWriterContext candidate : getChildren()) {
      candidate.addDenormDims();
    }
  }

  public void updateDimFilterWithFactFilter() throws LensException {
    for (QueryWriterContext candidate : getChildren()) {
      candidate.updateDimFilterWithFactFilter();
    }
  }

  @Override
  public QueryAST getQueryAst() {
    return getCubeQueryContext();
  }

  @Override
  public void updateFromString() throws LensException {
    for (QueryWriterContext queryWriterContext : getChildren()) {
      queryWriterContext.updateFromString();
    }
  }
  private List<StorageCandidateHQLContext> getLeafQueryWriterContexts() {
    List<StorageCandidateHQLContext> ret = Lists.newArrayList();
    for (QueryWriterContext queryWriterContext : getChildren()) {
      if (queryWriterContext instanceof MultiCandidateQueryWriterContext) {
        ret.addAll(((MultiCandidateQueryWriterContext) queryWriterContext).getLeafQueryWriterContexts());
      } else { // todo assert for checking third tpye
        ret.add((StorageCandidateHQLContext) queryWriterContext);
      }
    }
    return ret;
  }
  @Override
  public QueryWriter toQueryWriter() throws LensException {
    return new UnionQueryWriter(getLeafQueryWriterContexts(), getCubeQueryContext());
  }
}
