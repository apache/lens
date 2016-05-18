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

package org.apache.lens.cube.metadata;

import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.collect.Lists;

public class Segmentation extends AbstractCubeTable {

  private String cubeName;
  private final Set<Segment> segments;
  private static final List<FieldSchema> COLUMNS = new ArrayList<>();

  static {
    COLUMNS.add(new FieldSchema("dummy", "string", "dummy column"));
  }

  public Segmentation(Table hiveTable) {
    super(hiveTable);
    this.segments = getSegments(getName(), getProperties());
    this.cubeName = getCubeName(getName(), getProperties());
  }

  public Segmentation(String cubeName, String segmentName, Set<Segment> segments) {
    this(cubeName, segmentName, segments, 0L);
  }

  public Segmentation(String cubeName, String segmentName, Set<Segment> segments, double weight) {
    this(cubeName, segmentName, segments, weight, new HashMap<String, String>());
  }

  public Segmentation(String baseCube, String segmentName, Set<Segment> segments,
                      double weight, Map<String, String> properties) {
    super(segmentName, COLUMNS, properties, weight);
    this.cubeName = baseCube;
    this.segments = segments;
    addProperties();
  }

  public static Set<String> getSegmentNames(Set<Segment> segments) {
    Set<String> names = new HashSet<>();
    for (Segment seg : segments) {
      names.add(seg.getName());
    }
    return names;
  }

  @Override
  protected void addProperties() {
    super.addProperties();
    addCubeName(getName(), getProperties(), cubeName);
    addSegmentProperties(getName(), getProperties(), segments);
  }

  private static void addCubeName(String segName, Map<String, String> props, String cubeName) {
    props.put(MetastoreUtil.getSegmentationCubeNameKey(segName), cubeName);
  }

  private static void addSegmentProperties(String name, Map<String, String> props,
                                               Set<Segment> segments) {
    if (segments != null){
      props.put(MetastoreUtil.getSegmentsListKey(name),
          MetastoreUtil.getNamedStr(segments));
      for (Segment segment : segments) {
        for (Map.Entry<String, String> segProp : segment.getProperties().entrySet()) {
          if (!segProp.getKey().startsWith(MetastoreUtil.getSegmentPropertyKey(segment.getName()))) {
            props.put(MetastoreUtil.getSegmentPropertyKey(segment.getName()).concat(segProp.getKey()),
                segProp.getValue());
          }
        }
      }
    }
  }

  private static Set<Segment> getSegments(String name, Map<String, String> props) {
    Set<Segment> segments = new HashSet<>();
    String segmentsString =  MetastoreUtil.getNamedStringValue(props, MetastoreUtil.getSegmentsListKey(name));
    if (!StringUtils.isBlank(segmentsString)) {
      String[] segs = segmentsString.split(",");
      for (String seg : segs) {
        Map<String, String> segProps = new HashMap<>();
        String segmentPropStr =  MetastoreUtil.getSegmentPropertyKey(seg);
        for (String key : props.keySet()) {
          if (key.startsWith(segmentPropStr)){
            segProps.put(key, props.get(key));
          }
        }
        segments.add(new Segment(seg, segProps));
      }
    }
    return segments;
  }

  public void addSegment(Segment cubeSeg) {
    if (!segments.contains(cubeSeg)) {
      segments.add(cubeSeg);
      addSegmentProperties(getName(), getProperties(), segments);
    }
  }

  public void dropSegment(Segment cubeSeg) {
    if (segments.contains(cubeSeg)) {
      segments.remove(cubeSeg);
      addSegmentProperties(getName(), getProperties(), segments);
    }
  }

  public void alterSegment(Set<Segment> cubeSegs) {
    if (!segments.equals(cubeSegs)) {
      segments.clear();
      segments.addAll(cubeSegs);
      addSegmentProperties(getName(), getProperties(), segments);
    }
  }

  public void alterBaseCubeName(String cubeName) {
    this.cubeName = cubeName;
    addCubeName(getName(), getProperties(), cubeName);
  }

  public String getBaseCube() {
    return cubeName;
  }

  public Set<Segment> getSegments() {
    return segments;
  }

  @Override
  public CubeTableType getTableType() {
    return CubeTableType.SEGMENTATION;
  }

  @Override
  public Set<String> getStorages() {
    return null;
  }


  public Date getAbsoluteStartTime() {
    return getDateFromProperty(MetastoreConstants.SEGMENTATION_ABSOLUTE_START_TIME, false, true);
  }

  public Date getRelativeStartTime() {
    return getDateFromProperty(MetastoreConstants.SEGMENTATION_RELATIVE_START_TIME, true, true);
  }

  public Date getStartTime() {
    return Collections.max(Lists.newArrayList(getRelativeStartTime(), getAbsoluteStartTime()));
  }

  public Date getAbsoluteEndTime() {
    return getDateFromProperty(MetastoreConstants.SEGMENTATION_ABSOLUTE_END_TIME, false, false);
  }

  public Date getRelativeEndTime() {
    return getDateFromProperty(MetastoreConstants.SEGMENTATION_RELATIVE_END_TIME, true, false);
  }

  public Date getEndTime() {
    return Collections.min(Lists.newArrayList(getRelativeEndTime(), getAbsoluteEndTime()));
  }
  static String getCubeName(String segName, Map<String, String> props) {
    return props.get(MetastoreUtil.getSegmentationCubeNameKey(segName));
  }


}
