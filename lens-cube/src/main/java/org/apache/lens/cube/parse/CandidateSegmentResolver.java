package org.apache.lens.cube.parse;

import java.util.List;

import org.apache.lens.cube.metadata.Segmentation;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import com.google.common.collect.Lists;

/**
 * Created on 14/03/17.
 */
public class CandidateSegmentResolver implements ContextRewriter {
  private final PHASE phase;
  private Configuration conf;
  private HiveConf hconf;

  public CandidateSegmentResolver(Configuration conf, HiveConf hconf) {
    this.conf = conf;
    this.hconf = hconf;
    this.phase = PHASE.first();
  }

  enum PHASE {
    POPULATE, EXPLODE;

    static PHASE first() {
      return values()[0];
    }

    static PHASE last() {
      return values()[values().length - 1];
    }

    PHASE next() {
      return values()[(this.ordinal() + 1) % values().length];
    }
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
    List<SegmentationCandidate> segmentationCandidates = Lists.newArrayList();
    for (Segmentation segmentation : cubeql.getMetastoreClient().getAllSegmentations(cubeql.getCube())) {
      segmentationCandidates.add(new SegmentationCandidate(cubeql, segmentation, conf, hconf));
    }
    cubeql.getCandidates().addAll(segmentationCandidates);
  }
}
