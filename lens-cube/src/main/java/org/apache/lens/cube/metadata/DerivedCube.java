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

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.collect.Lists;

public class DerivedCube extends AbstractCubeTable implements CubeInterface {

  private static final List<FieldSchema> COLUMNS = new ArrayList<>();

  static {
    COLUMNS.add(new FieldSchema("dummy", "string", "dummy column"));
  }

  private final Cube parent;
  private final Set<String> measures = new HashSet<>();
  private final Set<String> dimensions = new HashSet<>();

  public DerivedCube(String name, Set<String> measures, Set<String> dimensions, Cube parent) throws LensException {
    this(name, measures, dimensions, new HashMap<String, String>(), 0L, parent);
  }

  public DerivedCube(String name, Set<String> measures, Set<String> dimensions, Map<String, String> properties,
    double weight, Cube parent) throws LensException {
    super(name, COLUMNS, properties, weight);
    for (String msr : measures) {
      this.measures.add(msr.toLowerCase());
    }
    for (String dim : dimensions) {
      this.dimensions.add(dim.toLowerCase());
    }
    this.parent = parent;
    validate();
    addProperties();
  }

  public void validate() throws LensException {
    List<String> measuresNotInParentCube = Lists.newArrayList();
    List<String> dimAttributesNotInParentCube = Lists.newArrayList();
    for (String msr : measures) {
      if (parent.getMeasureByName(msr) == null) {
        measuresNotInParentCube.add(msr);
      }
    }
    for (String dim : dimensions) {
      if (parent.getDimAttributeByName(dim) == null) {
        dimAttributesNotInParentCube.add(dim);
      }
    }
    StringBuilder validationErrorStringBuilder = new StringBuilder();
    String sep = "";
    boolean invalid = false;
    if (!measuresNotInParentCube.isEmpty()) {
      validationErrorStringBuilder.append(sep).append("Measures ").append(measuresNotInParentCube);
      sep = " and ";
      invalid = true;
    }
    if (!dimAttributesNotInParentCube.isEmpty()) {
      validationErrorStringBuilder.append(sep).append("Dim Attributes ").append(dimAttributesNotInParentCube);
      invalid = true;
    }
    if (invalid) {
      throw new LensException(LensCubeErrorCode.ERROR_IN_ENTITY_DEFINITION.getLensErrorInfo(),
        "Derived cube invalid: " + validationErrorStringBuilder.append(" were not present in " + "parent cube ")
          .append(parent));
    }
  }

  public DerivedCube(Table tbl, Cube parent) {
    super(tbl);
    this.measures.addAll(getMeasures(getName(), getProperties()));
    this.dimensions.addAll(getDimensions(getName(), getProperties()));
    this.parent = parent;
  }

  private Set<CubeMeasure> cachedMeasures = new HashSet<>();
  private Set<CubeDimAttribute> cachedDims = new HashSet<>();

  public Set<CubeMeasure> getMeasures() {
    synchronized (measures) {
      if (cachedMeasures.isEmpty()) {
        for (String msr : measures) {
          cachedMeasures.add(parent.getMeasureByName(msr));
        }
      }
    }
    return cachedMeasures;
  }

  public Set<CubeDimAttribute> getDimAttributes() {
    synchronized (dimensions) {
      if (cachedDims.isEmpty()) {
        for (String dim : dimensions) {
          cachedDims.add(parent.getDimAttributeByName(dim));
        }
      }
    }
    return cachedDims;
  }

  @Override
  public CubeTableType getTableType() {
    return CubeTableType.CUBE;
  }

  @Override
  public Set<String> getStorages() {
    return null;
  }

  @Override
  public void addProperties() {
    super.addProperties();
    updateMeasureProperties();
    updateDimAttributeProperties();
    getProperties().put(MetastoreUtil.getParentCubeNameKey(getName()), parent.getName().toLowerCase());
    getProperties().put(MetastoreUtil.getParentCubeNameKey(getName()), parent.getName().toLowerCase());
  }
  public void updateDimAttributeProperties() {
    MetastoreUtil.addNameStrings(getProperties(), MetastoreUtil.getCubeDimensionListKey(getName()),
      MetastoreUtil.getNamedSetFromStringSet(dimensions));
  }
  public void updateMeasureProperties() {
    MetastoreUtil.addNameStrings(getProperties(), MetastoreUtil.getCubeMeasureListKey(getName()),
      MetastoreUtil.getNamedSetFromStringSet(measures));
  }

  public static Set<String> getMeasures(String name, Map<String, String> props) {
    Set<String> measures = new HashSet<>();
    String measureStr = MetastoreUtil.getNamedStringValue(props, MetastoreUtil.getCubeMeasureListKey(name));
    measures.addAll(Arrays.asList(StringUtils.split(measureStr, ',')));
    return measures;
  }

  public Set<String> getTimedDimensions() {
    String str = getProperties().get(MetastoreUtil.getCubeTimedDimensionListKey(getName()));
    if (str != null) {
      Set<String> timedDimensions = new HashSet<>();
      timedDimensions.addAll(Arrays.asList(StringUtils.split(str, ',')));
      return timedDimensions;
    } else {
      return parent.getTimedDimensions();
    }
  }

  public static Set<String> getDimensions(String name, Map<String, String> props) {
    Set<String> dimensions = new HashSet<>();
    String dimStr = MetastoreUtil.getNamedStringValue(props, MetastoreUtil.getCubeDimensionListKey(name));
    dimensions.addAll(Arrays.asList(StringUtils.split(dimStr, ',')));
    return dimensions;
  }

  public Cube getParent() {
    return parent;
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
    DerivedCube other = (DerivedCube) obj;
    if (!this.getParent().equals(other.getParent())) {
      return false;
    }
    if (this.getMeasureNames() == null) {
      if (other.getMeasureNames() != null) {
        return false;
      }
    } else if (!this.getMeasureNames().equals(other.getMeasureNames())) {
      return false;
    }
    if (this.getDimAttributeNames() == null) {
      if (other.getDimAttributeNames() != null) {
        return false;
      }
    } else if (!this.getDimAttributeNames().equals(other.getDimAttributeNames())) {
      return false;
    }
    return true;
  }

  public CubeDimAttribute getDimAttributeByName(String dimension) {
    if (dimensions.contains(dimension.toLowerCase())) {
      return parent.getDimAttributeByName(dimension);
    }
    return null;
  }

  public CubeMeasure getMeasureByName(String measure) {
    if (measures.contains(measure.toLowerCase())) {
      return parent.getMeasureByName(measure);
    }
    return null;
  }

  public CubeColumn getColumnByName(String column) {
    CubeColumn cubeCol = (CubeColumn) getMeasureByName(column);
    if (cubeCol == null) {
      cubeCol = (CubeColumn) getDimAttributeByName(column);
    }
    return cubeCol;
  }

  /**
   * Add a new measure
   *
   * @param measure measure name
   */
  public void addMeasure(String measure) {
    measures.add(measure.toLowerCase());
    updateMeasureProperties();
  }

  /**
   * Add a new dimension
   *
   * @param dimension attribute name
   */
  public void addDimension(String dimension) {
    dimensions.add(dimension.toLowerCase());
    updateDimAttributeProperties();
  }

  /**
   * Remove the dimension with name specified
   *
   * @param dimName
   */
  public void removeDimension(String dimName) {
    dimensions.remove(dimName.toLowerCase());
    updateDimAttributeProperties();
  }

  /**
   * Remove the measure with name specified
   *
   * @param msrName
   */
  public void removeMeasure(String msrName) {
    measures.remove(msrName.toLowerCase());
    updateMeasureProperties();
  }

  @Override
  public boolean isDerivedCube() {
    return true;
  }

  @Override
  public Set<String> getMeasureNames() {
    return measures;
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
    return true;
  }

  @Override
  public Set<ExprColumn> getExpressions() {
    return null;
  }

  @Override
  public ExprColumn getExpressionByName(String exprName) {
    return null;
  }

  @Override
  public Set<String> getAllFieldNames() {
    Set<String> fieldNames = new HashSet<>();
    fieldNames.addAll(getMeasureNames());
    fieldNames.addAll(getDimAttributeNames());
    fieldNames.addAll(getTimedDimensions());
    return fieldNames;
  }

  @Override
  public Set<String> getExpressionNames() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Set<JoinChain> getJoinChains() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public JoinChain getChainByName(String chainName) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Set<String> getJoinChainNames() {
    // TODO Auto-generated method stub
    return null;
  }
}
