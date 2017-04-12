package org.apache.lens.cube.parse;

import org.apache.lens.server.api.error.LensException;

/**
 * Created on 11/04/17.
 */
public class CandidateExploder implements ContextRewriter {
  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
    cubeql.getCandidates().removeIf(this::shouldBeRemoved);
  }

  private boolean shouldBeRemoved(Candidate candidate) {
    if (candidate.getChildren() == null) {
      return false;
    } else if (candidate instanceof SegmentationCandidate) {
      try {
        boolean areCandidatsPicked = (((SegmentationCandidate) candidate).rewriteInternal());
        return !areCandidatsPicked;
      } catch (LensException e) {
        return true;
      }
    } else {
      return candidate.getChildren().stream().anyMatch(this::shouldBeRemoved);
    }
  }
}
