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
  private final boolean enabled;
  private Configuration conf;
  private HiveConf hconf;

  public CandidateSegmentResolver(Configuration conf, HiveConf hconf) {
    this.conf = conf;
    this.hconf = hconf;
    this.enabled = conf.getBoolean(CubeQueryConfUtil.RESOLVE_SEGMENTATIONS, true);
  }


  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
    if (enabled) {
      List<SegmentationCandidate> segmentationCandidates = Lists.newArrayList();
      for (Segmentation segmentation : cubeql.getMetastoreClient().getAllSegmentations(cubeql.getCube())) {
        segmentationCandidates.add(new SegmentationCandidate(cubeql, segmentation, conf, hconf));
      }
      cubeql.getCandidates().addAll(segmentationCandidates);
    }
  }
}
