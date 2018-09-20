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

import java.lang.reflect.Constructor;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Cube extends AbstractBaseTable implements CubeInterface {
  private final Set<CubeMeasure> measures;
  private final Set<CubeDimAttribute> dimensions;
  private final Map<String, CubeMeasure> measureMap;
  private final Map<String, CubeDimAttribute> dimMap;


  public Cube(String name, Set<CubeMeasure> measures, Set<CubeDimAttribute> dimensions) {
    this(name, measures, dimensions, new HashMap<String, String>());
  }

  public Cube(String name, Set<CubeMeasure> measures, Set<CubeDimAttribute> dimensions,
    Map<String, String> properties) {
    this(name, measures, dimensions, properties, 0L);
  }

  public Cube(String name, Set<CubeMeasure> measures, Set<CubeDimAttribute> dimensions, Map<String, String> properties,
    double weight) {
    this(name, measures, dimensions, null, null, properties, weight);
  }

  public Cube(String name, Set<CubeMeasure> measures, Set<CubeDimAttribute> dimensions, Set<ExprColumn> expressions,
    Set<JoinChain> joinChains, Map<String, String> properties, double weight) {
    super(name, expressions, joinChains, properties, weight);
    this.measures = measures;
    this.dimensions = dimensions;

    measureMap = new HashMap<>();
    for (CubeMeasure m : measures) {
      measureMap.put(m.getName().toLowerCase(), m);
    }

    dimMap = new HashMap<>();
    for (CubeDimAttribute dim : dimensions) {
      dimMap.put(dim.getName().toLowerCase(), dim);
    }

    addProperties();
  }

  public Cube(Table tbl) {
    super(tbl);
    this.measures = getMeasures(getName(), getProperties());
    this.dimensions = getDimensions(getName(), getProperties());

    measureMap = new HashMap<>();
    for (CubeMeasure m : measures) {
      measureMap.put(m.getName().toLowerCase(), m);
    }

    dimMap = new HashMap<>();
    for (CubeDimAttribute dim : dimensions) {
      addAllDimsToMap(dim);
    }
  }

  private void addAllDimsToMap(CubeDimAttribute dim) {
    dimMap.put(dim.getName().toLowerCase(), dim);
    if (dim instanceof HierarchicalDimAttribute) {
      for (CubeDimAttribute d : ((HierarchicalDimAttribute) dim).getHierarchy()) {
        addAllDimsToMap(d);
      }
    }
  }

  public Set<CubeMeasure> getMeasures() {
    return measures;
  }
  public Optional<Date> getColumnStartTime(String column) {
    return Optional.ofNullable(getColumnByName(column)).map(CubeColumn::getStartTime);
  }
  public Optional<Date> getColumnEndTime(String column) {
    return Optional.ofNullable(getColumnByName(column)).map(CubeColumn::getEndTime);
  }

  public Set<CubeDimAttribute> getDimAttributes() {
    return dimensions;
  }

  public Set<String> getTimedDimensions() {
    String str = getProperties().get(MetastoreUtil.getCubeTimedDimensionListKey(getName()));
    Set<String> timedDimensions = new HashSet<>();
    if (str != null) {
      timedDimensions.addAll(Arrays.asList(StringUtils.split(str, ',')));
    }
    return timedDimensions;
  }

  @Override
  public CubeTableType getTableType() {
    return CubeTableType.CUBE;
  }

  @Override
  public void addProperties() {
    super.addProperties();
    MetastoreUtil.addNameStrings(getProperties(), MetastoreUtil.getCubeMeasureListKey(getName()), measures);
    setMeasureProperties(getProperties(), measures);
    MetastoreUtil.addNameStrings(getProperties(), MetastoreUtil.getCubeDimensionListKey(getName()), dimensions);
    setDimensionProperties(getProperties(), dimensions);
  }

  private static void setMeasureProperties(Map<String, String> props, Set<CubeMeasure> measures) {
    for (CubeMeasure measure : measures) {
      measure.addProperties(props);
    }
  }

  private static void setDimensionProperties(Map<String, String> props, Set<CubeDimAttribute> dimensions) {
    for (CubeDimAttribute dimension : dimensions) {
      dimension.addProperties(props);
    }
  }

  public static Set<CubeMeasure> getMeasures(String name, Map<String, String> props) {
    Set<CubeMeasure> measures = new HashSet<>();
    String measureStr = MetastoreUtil.getNamedStringValue(props, MetastoreUtil.getCubeMeasureListKey(name));
    String[] names = measureStr.split(",");
    for (String measureName : names) {
      String className = props.get(MetastoreUtil.getMeasureClassPropertyKey(measureName));
      CubeMeasure measure;
      try {
        Class<?> clazz = Class.forName(className);
        Constructor<?> constructor;
        constructor = clazz.getConstructor(String.class, Map.class);
        measure = (CubeMeasure) constructor.newInstance(new Object[]{measureName, props});
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid measure", e);
      }
      measures.add(measure);
    }
    return measures;
  }

  public static Set<CubeDimAttribute> getDimensions(String name, Map<String, String> props) {
    Set<CubeDimAttribute> dimensions = new HashSet<>();
    String dimStr = MetastoreUtil.getNamedStringValue(props, MetastoreUtil.getCubeDimensionListKey(name));
    if (StringUtils.isNotBlank(dimStr)) {
      String[] names = dimStr.split(",");
      for (String dimName : names) {
        String className = props.get(MetastoreUtil.getDimensionClassPropertyKey(dimName));
        CubeDimAttribute dim;
        try {
          Class<?> clazz = Class.forName(className);
          Constructor<?> constructor;
          constructor = clazz.getConstructor(String.class, Map.class);
          dim = (CubeDimAttribute) constructor.newInstance(new Object[]{dimName, props});
        } catch (Exception e) {
          throw new IllegalArgumentException("Invalid dimension", e);
        }
        dimensions.add(dim);
      }
    }
    return dimensions;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    Cube other = (Cube) obj;
    if (this.getMeasures() == null) {
      if (other.getMeasures() != null) {
        return false;
      }
    } else if (!this.getMeasures().equals(other.getMeasures())) {
      return false;
    }
    if (this.getDimAttributes() == null) {
      if (other.getDimAttributes() != null) {
        return false;
      }
    } else if (!this.getDimAttributes().equals(other.getDimAttributes())) {
      return false;
    }
    return true;
  }

  public CubeDimAttribute getDimAttributeByName(String dimension) {
    return dimMap.get(dimension == null ? null : dimension.toLowerCase());
  }

  public CubeMeasure getMeasureByName(String measure) {
    return measureMap.get(measure == null ? null : measure.toLowerCase());
  }

  public CubeColumn getColumnByName(String column) {
    CubeColumn cubeCol = super.getExpressionByName(column);
    if (cubeCol == null) {
      cubeCol = getMeasureByName(column);
      if (cubeCol == null) {
        cubeCol = getDimAttributeByName(column);
      }
    }
    return cubeCol;
  }

  /**
   * Alters the measure if already existing or just adds if it is new measure.
   *
   * @param measure new measure definition
   */
  public void alterMeasure(@NonNull CubeMeasure measure) {
    // Replace measure if already existing
    if (measureMap.containsKey(measure.getName().toLowerCase())) {
      measures.remove(getMeasureByName(measure.getName()));
      log.info("Replacing measure {} with {}", getMeasureByName(measure.getName()), measure);
    }

    measures.add(measure);
    measureMap.put(measure.getName().toLowerCase(), measure);
    MetastoreUtil.addNameStrings(getProperties(), MetastoreUtil.getCubeMeasureListKey(getName()), measures);
    measure.addProperties(getProperties());
  }

  /**
   * Remove the joinchain with name specified
   *
   * @param chainName chain name
   */
  public boolean removeJoinChain(@NonNull String chainName) {
    if (super.removeJoinChain(chainName)) {
      log.info("Removing dimension {}", getDimAttributeByName(chainName));
      return true;
    }
    return false;
  }

  /**
   * Alters the dimension if already existing or just adds if it is new dimension
   *
   * @param dimension the dim attribute
   */
  public void alterDimension(@NonNull CubeDimAttribute dimension) {
    // Replace dimension if already existing
    if (dimMap.containsKey(dimension.getName().toLowerCase())) {
      dimensions.remove(getDimAttributeByName(dimension.getName()));
      log.info("Replacing dimension {} with {}", getDimAttributeByName(dimension.getName()), dimension);
    }

    dimensions.add(dimension);
    dimMap.put(dimension.getName().toLowerCase(), dimension);
    MetastoreUtil.addNameStrings(getProperties(), MetastoreUtil.getCubeDimensionListKey(getName()), dimensions);
    dimension.addProperties(getProperties());
  }


  /**
   * Remove the attribute with name specified
   *
   * @param dimName attribute name
   */
  public void removeDimension(@NonNull String dimName) {
    if (dimMap.containsKey(dimName.toLowerCase())) {
      log.info("Removing dimension {}", getDimAttributeByName(dimName));
      dimensions.remove(getDimAttributeByName(dimName));
      dimMap.remove(dimName.toLowerCase());
      MetastoreUtil.addNameStrings(getProperties(), MetastoreUtil.getCubeDimensionListKey(getName()), dimensions);
    }
  }

  /**
   * Remove the measure with name specified
   *
   * @param msrName measure name
   */
  public void removeMeasure(@NonNull String msrName) {
    if (measureMap.containsKey(msrName.toLowerCase())) {
      log.info("Removing measure {}", getMeasureByName(msrName));
      measures.remove(getMeasureByName(msrName));
      measureMap.remove(msrName.toLowerCase());
      MetastoreUtil.addNameStrings(getProperties(), MetastoreUtil.getCubeMeasureListKey(getName()), measures);
    }
  }

  /**
   * Adds the timed dimension
   *
   * @param timedDimension time dimension
   */
  public void addTimedDimension(@NonNull String timedDimension) {
    timedDimension = timedDimension.toLowerCase();
    Set<String> timeDims = getTimedDimensions();
    if (timeDims == null) {
      timeDims = new LinkedHashSet<>();
    }
    if (timeDims.contains(timedDimension)) {
      log.info("Timed dimension {} is already present in cube {}", timedDimension, getName());
      return;
    }

    timeDims.add(timedDimension);
    getProperties().put(MetastoreUtil.getCubeTimedDimensionListKey(getName()), StringUtils.join(timeDims, ","));
  }

  /**
   * Removes the timed dimension
   *
   * @param timedDimension time dimension
   */
  public void removeTimedDimension(@NonNull String timedDimension) {
    timedDimension = timedDimension.toLowerCase();
    Set<String> timeDims = getTimedDimensions();
    if (timeDims != null && timeDims.contains(timedDimension)) {
      timeDims.remove(timedDimension);
      getProperties().put(MetastoreUtil.getCubeTimedDimensionListKey(getName()), StringUtils.join(timeDims, ","));
    }
  }

  @Override
  public boolean isDerivedCube() {
    return false;
  }

  @Override
  public Set<String> getMeasureNames() {
    Set<String> measureNames = new HashSet<>();
    for (CubeMeasure f : getMeasures()) {
      measureNames.add(f.getName().toLowerCase());
    }
    return measureNames;
  }

  @Override
  public Set<String> getDimAttributeNames() {
    Set<String> dimNames = new HashSet<>();
    for (CubeDimAttribute f : getDimAttributes()) {
      MetastoreUtil.addColumnNames(f, dimNames);
    }
    return dimNames;
  }

  @Override
  public boolean allFieldsQueriable() {
    String canBeQueried = getProperties().get(MetastoreConstants.CUBE_ALL_FIELDS_QUERIABLE);
    return canBeQueried == null || Boolean.parseBoolean(canBeQueried.toLowerCase());
  }

  @Override
  public Set<String> getAllFieldNames() {
    Set<String> fieldNames = super.getAllFieldNames();
    fieldNames.addAll(getMeasureNames());
    fieldNames.addAll(getDimAttributeNames());
    fieldNames.addAll(getTimedDimensions());
    return fieldNames;
  }

  public Set<CubeColumn> getAllFields() {
    Set<CubeColumn> columns = Sets.newHashSet();
    columns.addAll(getMeasures());
    columns.addAll(getDimAttributes());
    columns.addAll(getExpressions());
    return columns;
  }

  /**
   * @see org.apache.lens.cube.metadata.AbstractBaseTable
   */
  @Override
  protected String getJoinChainListPropKey(@NonNull String tblname) {
    return MetastoreUtil.getCubeJoinChainListKey(getName());
  }

  public String getPartitionColumnOfTimeDim(@NonNull String timeDimName) {
    String partCol = getProperties().get(MetastoreConstants.TIMEDIM_TO_PART_MAPPING_PFX + timeDimName);
    return StringUtils.isNotBlank(partCol) ? partCol : timeDimName;
  }

  public String getTimeDimOfPartitionColumn(@NonNull String partCol) {
    Map<String, String> properties = getProperties();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (entry.getKey().startsWith(MetastoreConstants.TIMEDIM_TO_PART_MAPPING_PFX)
        && entry.getValue().equalsIgnoreCase(partCol)) {
        return entry.getKey().replace(MetastoreConstants.TIMEDIM_TO_PART_MAPPING_PFX, "");
      }
    }
    return partCol;
  }

  @Override
  public Date getAbsoluteStartTime() {
    return MetastoreUtil.getDateFromProperty(this.getProperties().get(MetastoreConstants.CUBE_ABSOLUTE_START_TIME),
      false, true);
  }

  @Override
  public Date getRelativeStartTime() {
    return MetastoreUtil.getDateFromProperty(this.getProperties().get(MetastoreConstants.CUBE_RELATIVE_START_TIME),
      true, true);
  }

  @Override
  public Date getStartTime() {
    return Collections.max(Lists.newArrayList(getRelativeStartTime(), getAbsoluteStartTime()));
  }

  @Override
  public Date getAbsoluteEndTime() {
    return MetastoreUtil.getDateFromProperty(this.getProperties().get(MetastoreConstants.CUBE_ABSOLUTE_END_TIME),
      false, false);
  }

  @Override
  public Date getRelativeEndTime() {
    return MetastoreUtil.getDateFromProperty(this.getProperties().get(MetastoreConstants.CUBE_RELATIVE_END_TIME),
      true, false);
  }

  @Override
  public Date getEndTime() {
    return Collections.min(Lists.newArrayList(getRelativeEndTime(), getAbsoluteEndTime()));
  }


}
