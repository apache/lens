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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import lombok.Getter;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

public class Cube extends AbstractBaseTable implements CubeInterface {
  private final Set<CubeMeasure> measures;
  private final Set<CubeDimAttribute> dimensions;
  @Getter private final Set<JoinChain> joinChains;
  private final Map<String, CubeMeasure> measureMap;
  private final Map<String, CubeDimAttribute> dimMap;
  private final Map<String, JoinChain> chainMap;

  public Cube(String name, Set<CubeMeasure> measures, Set<CubeDimAttribute> dimensions) {
    this(name, measures, dimensions, new HashMap<String, String>());
  }

  public Cube(String name, Set<CubeMeasure> measures, Set<CubeDimAttribute> dimensions, Map<String, String> properties) {
    this(name, measures, dimensions, properties, 0L);
  }

  public Cube(String name, Set<CubeMeasure> measures, Set<CubeDimAttribute> dimensions, Map<String, String> properties,
      double weight) {
    this(name, measures, dimensions, null, null, properties, weight);
  }

  public Cube(String name, Set<CubeMeasure> measures, Set<CubeDimAttribute> dimensions, Set<ExprColumn> expressions,
      Set<JoinChain> joinChains,
      Map<String, String> properties, double weight) {
    super(name, expressions, properties, weight);
    this.measures = measures;
    this.dimensions = dimensions;
    if (joinChains != null) {
      this.joinChains = joinChains;
    } else {
      this.joinChains = new HashSet<JoinChain>();
    }

    measureMap = new HashMap<String, CubeMeasure>();
    for (CubeMeasure m : measures) {
      measureMap.put(m.getName().toLowerCase(), m);
    }

    dimMap = new HashMap<String, CubeDimAttribute>();
    for (CubeDimAttribute dim : dimensions) {
      dimMap.put(dim.getName().toLowerCase(), dim);
    }

    chainMap = new HashMap<String, JoinChain>();
    for (JoinChain chain : this.joinChains) {
      chainMap.put(chain.getName().toLowerCase(), chain);
    }

    addProperties();
  }

  public Cube(Table tbl) {
    super(tbl);
    this.measures = getMeasures(getName(), getProperties());
    this.dimensions = getDimensions(getName(), getProperties());
    this.joinChains = getJoinChains(getName(), getProperties());
    measureMap = new HashMap<String, CubeMeasure>();
    for (CubeMeasure m : measures) {
      measureMap.put(m.getName().toLowerCase(), m);
    }

    dimMap = new HashMap<String, CubeDimAttribute>();
    for (CubeDimAttribute dim : dimensions) {
      addAllDimsToMap(dim);
    }

    chainMap = new HashMap<String, JoinChain>();
    for (JoinChain chain : joinChains) {
      chainMap.put(chain.getName().toLowerCase(), chain);
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

  public Set<CubeDimAttribute> getDimAttributes() {
    return dimensions;
  }

  public Set<String> getTimedDimensions() {
    String str = getProperties().get(MetastoreUtil.getCubeTimedDimensionListKey(getName()));
    Set<String> timedDimensions = new HashSet<String>();
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
    MetastoreUtil.addNameStrings(getProperties(), MetastoreUtil.getCubeJoinChainListKey(getName()), joinChains);
    setJoinChainProperties(getProperties(), joinChains);
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

  private static void setJoinChainProperties(Map<String, String> props, Set<JoinChain> chains) {
    for (JoinChain chain : chains) {
      chain.addProperties(props);
    }
  }

  public static Set<CubeMeasure> getMeasures(String name, Map<String, String> props) {
    Set<CubeMeasure> measures = new HashSet<CubeMeasure>();
    String measureStr = MetastoreUtil.getNamedStringValue(props, MetastoreUtil.getCubeMeasureListKey(name));
    String[] names = measureStr.split(",");
    for (String measureName : names) {
      String className = props.get(MetastoreUtil.getMeasureClassPropertyKey(measureName));
      CubeMeasure measure;
      try {
        Class<?> clazz = Class.forName(className);
        Constructor<?> constructor;
        constructor = clazz.getConstructor(String.class, Map.class);
        measure = (CubeMeasure) constructor.newInstance(new Object[] { measureName, props });
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid measure", e);
      }
      measures.add(measure);
    }
    return measures;
  }

  public static Set<CubeDimAttribute> getDimensions(String name, Map<String, String> props) {
    Set<CubeDimAttribute> dimensions = new HashSet<CubeDimAttribute>();
    String dimStr = MetastoreUtil.getNamedStringValue(props, MetastoreUtil.getCubeDimensionListKey(name));
    String[] names = dimStr.split(",");
    for (String dimName : names) {
      String className = props.get(MetastoreUtil.getDimensionClassPropertyKey(dimName));
      CubeDimAttribute dim;
      try {
        Class<?> clazz = Class.forName(className);
        Constructor<?> constructor;
        constructor = clazz.getConstructor(String.class, Map.class);
        dim = (CubeDimAttribute) constructor.newInstance(new Object[] { dimName, props });
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid dimension", e);
      }
      dimensions.add(dim);
    }
    return dimensions;
  }

  private static Set<JoinChain> getJoinChains(String name, Map<String, String> props) {
    Set<JoinChain> joinChains = new HashSet<JoinChain>();
    String joinChainsStr = MetastoreUtil.getNamedStringValue(props, MetastoreUtil.getCubeJoinChainListKey(name));
    if (!StringUtils.isBlank(joinChainsStr)) {
      String[] cnames = joinChainsStr.split(",");
      for (String chainName : cnames) {
        JoinChain chain = new JoinChain(chainName, props);
        joinChains.add(chain);
      }
    }
    return joinChains;
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
    if (this.getJoinChains() == null) {
      if (other.getJoinChains() != null) {
        return false;
      }
    } else if (!this.getJoinChains().equals(other.getJoinChains())) {
      return false;
    }
    return true;
  }

  public CubeDimAttribute getDimAttributeByName(String dimension) {
    return dimMap.get(dimension == null ? dimension : dimension.toLowerCase());
  }

  public CubeMeasure getMeasureByName(String measure) {
    return measureMap.get(measure == null ? measure : measure.toLowerCase());
  }

  public CubeColumn getColumnByName(String column) {
    CubeColumn cubeCol = (CubeColumn) super.getExpressionByName(column);
    if (cubeCol == null) {
      cubeCol = (CubeColumn) getMeasureByName(column);
      if (cubeCol == null) {
        cubeCol = (CubeColumn) getDimAttributeByName(column);
      }
    }
    return cubeCol;
  }

  public JoinChain getChainByName(String name) {
    return chainMap.get(name == null ? name : name.toLowerCase());
  }

  /**
   * Alters the measure if already existing or just adds if it is new measure.
   * 
   * @param measure
   * @throws HiveException
   */
  public void alterMeasure(CubeMeasure measure) throws HiveException {
    if (measure == null) {
      throw new NullPointerException("Cannot add null measure");
    }

    // Replace measure if already existing
    if (measureMap.containsKey(measure.getName().toLowerCase())) {
      measures.remove(getMeasureByName(measure.getName()));
      LOG.info("Replacing measure " + getMeasureByName(measure.getName()) + " with " + measure);
    }

    measures.add(measure);
    measureMap.put(measure.getName().toLowerCase(), measure);
    MetastoreUtil.addNameStrings(getProperties(), MetastoreUtil.getCubeMeasureListKey(getName()), measures);
    measure.addProperties(getProperties());
  }

  /**
   * Alters the dimension if already existing or just adds if it is new
   * dimension
   * 
   * @param dimension
   * @throws HiveException
   */
  public void alterDimension(CubeDimAttribute dimension) throws HiveException {
    if (dimension == null) {
      throw new NullPointerException("Cannot add null dimension");
    }

    // Replace dimension if already existing
    if (dimMap.containsKey(dimension.getName().toLowerCase())) {
      dimensions.remove(getDimAttributeByName(dimension.getName()));
      LOG.info("Replacing dimension " + getDimAttributeByName(dimension.getName()) + " with " + dimension);
    }

    dimensions.add(dimension);
    dimMap.put(dimension.getName().toLowerCase(), dimension);
    MetastoreUtil.addNameStrings(getProperties(), MetastoreUtil.getCubeDimensionListKey(getName()), dimensions);
    dimension.addProperties(getProperties());
  }

  /**
   * Alters the joinchain if already existing or just adds if it is new chain
   *
   * @param joinchain
   * @throws HiveException
   */
  public void alterJoinChain(JoinChain joinchain) throws HiveException {
    if (joinchain == null) {
      throw new NullPointerException("Cannot add null joinchain");
    }

    // Replace dimension if already existing
    if (chainMap.containsKey(joinchain.getName().toLowerCase())) {
      joinChains.remove(getChainByName(joinchain.getName()));
      LOG.info("Replacing joinchain " + getChainByName(joinchain.getName()) + " with " + joinchain);
    }

    joinChains.add(joinchain);
    chainMap.put(joinchain.getName().toLowerCase(), joinchain);
    MetastoreUtil.addNameStrings(getProperties(), MetastoreUtil.getCubeJoinChainListKey(getName()), joinChains);
    joinchain.addProperties(getProperties());
  }

  /**
   * Remove the joinchain with name specified
   *
   * @param chainName
   */
  public void removeJoinChain(String chainName) {
    if (chainMap.containsKey(chainName.toLowerCase())) {
      LOG.info("Removing dimension " + getDimAttributeByName(chainName));
      joinChains.remove(getChainByName(chainName));
      chainMap.remove(chainName.toLowerCase());
      MetastoreUtil.addNameStrings(getProperties(), MetastoreUtil.getCubeJoinChainListKey(getName()), joinChains);
    }
  }

  /**
   * Remove the dimension with name specified
   * 
   * @param dimName
   */
  public void removeDimension(String dimName) {
    if (dimMap.containsKey(dimName.toLowerCase())) {
      LOG.info("Removing dimension " + getDimAttributeByName(dimName));
      dimensions.remove(getDimAttributeByName(dimName));
      dimMap.remove(dimName.toLowerCase());
      MetastoreUtil.addNameStrings(getProperties(), MetastoreUtil.getCubeDimensionListKey(getName()), dimensions);
    }
  }

  /**
   * Remove the measure with name specified
   * 
   * @param msrName
   */
  public void removeMeasure(String msrName) {
    if (measureMap.containsKey(msrName.toLowerCase())) {
      LOG.info("Removing measure " + getMeasureByName(msrName));
      measures.remove(getMeasureByName(msrName));
      measureMap.remove(msrName.toLowerCase());
      MetastoreUtil.addNameStrings(getProperties(), MetastoreUtil.getCubeMeasureListKey(getName()), measures);
    }
  }

  /**
   * Adds the timed dimension
   * 
   * @param timedDimension
   * @throws HiveException
   */
  public void addTimedDimension(String timedDimension) throws HiveException {
    if (timedDimension == null || timedDimension.isEmpty()) {
      throw new HiveException("Invalid timed dimension " + timedDimension);
    }
    timedDimension = timedDimension.toLowerCase();
    Set<String> timeDims = getTimedDimensions();
    if (timeDims == null) {
      timeDims = new LinkedHashSet<String>();
    }
    if (timeDims.contains(timedDimension)) {
      LOG.info("Timed dimension " + timedDimension + " is" + " already present in cube " + getName());
      return;
    }

    timeDims.add(timedDimension);
    getProperties().put(MetastoreUtil.getCubeTimedDimensionListKey(getName()), StringUtils.join(timeDims, ","));
  }

  /**
   * Removes the timed dimension
   * 
   * @param timedDimension
   * @throws HiveException
   */
  public void removeTimedDimension(String timedDimension) throws HiveException {
    if (timedDimension == null || timedDimension.isEmpty()) {
      throw new HiveException("Invalid timed dimension " + timedDimension);
    }
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
    Set<String> measureNames = new HashSet<String>();
    for (CubeMeasure f : getMeasures()) {
      measureNames.add(f.getName().toLowerCase());
    }
    return measureNames;
  }

  @Override
  public Set<String> getDimAttributeNames() {
    Set<String> dimNames = new HashSet<String>();
    for (CubeDimAttribute f : getDimAttributes()) {
      MetastoreUtil.addColumnNames(f, dimNames);
    }
    return dimNames;
  }

  @Override
  public Set<String> getJoinChainNames() {
    Set<String> chainNames = new HashSet<String>();
    for (JoinChain f : getJoinChains()) {
      chainNames.add(f.getName().toLowerCase());
    }
    return chainNames;
  }

  @Override
  public boolean allFieldsQueriable() {
    String canbeQueried = getProperties().get(MetastoreConstants.CUBE_ALL_FIELDS_QUERIABLE);
    if (canbeQueried != null) {
      return Boolean.parseBoolean(canbeQueried);
    }
    return true;
  }

  @Override
  public Set<String> getAllFieldNames() {
    Set<String> fieldNames = super.getAllFieldNames();
    fieldNames.addAll(getMeasureNames());
    fieldNames.addAll(getDimAttributeNames());
    fieldNames.addAll(getTimedDimensions());
    return fieldNames;
  }

  public String getPartitionColumnOfTimeDim(String timeDimName) {
    String partCol = getProperties().get(MetastoreConstants.TIMEDIM_TO_PART_MAPPING_PFX + timeDimName);
    return StringUtils.isNotBlank(partCol) ? partCol : timeDimName;
  }

  public String getTimeDimOfPartitionColumn(String partCol) {
    Map<String, String> properties = getProperties();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (entry.getKey().startsWith(MetastoreConstants.TIMEDIM_TO_PART_MAPPING_PFX)
          && entry.getValue().equalsIgnoreCase(partCol)) {
        String timeDim = entry.getKey().replace(MetastoreConstants.TIMEDIM_TO_PART_MAPPING_PFX, "");
        return timeDim;
      }
    }
    return partCol;
  }
}
