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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

public class DerivedCube extends AbstractCubeTable implements CubeInterface {

  private static final List<FieldSchema> columns = new ArrayList<FieldSchema>();
  static {
    columns.add(new FieldSchema("dummy", "string", "dummy column"));
  }

  private final Cube parent;
  private final Set<String> measures = new HashSet<String>();
  private final Set<String> dimensions = new HashSet<String>();

  public DerivedCube(String name, Set<String> measures, Set<String> dimensions, Cube parent) {
    this(name, measures, dimensions, new HashMap<String, String>(), 0L, parent);
  }

  public DerivedCube(String name, Set<String> measures, Set<String> dimensions, Map<String, String> properties,
      double weight, Cube parent) {
    super(name, columns, properties, weight);
    for (String msr : measures) {
      this.measures.add(msr.toLowerCase());
    }
    for (String dim : dimensions) {
      this.dimensions.add(dim.toLowerCase());
    }
    this.parent = parent;

    addProperties();
  }

  public DerivedCube(Table tbl, Cube parent) {
    super(tbl);
    this.measures.addAll(getMeasures(getName(), getProperties()));
    this.dimensions.addAll(getDimensions(getName(), getProperties()));
    this.parent = parent;
  }

  private Set<CubeMeasure> cachedMeasures;
  private Set<CubeDimAttribute> cachedDims;

  public Set<CubeMeasure> getMeasures() {
    if (cachedMeasures == null) {
      cachedMeasures = new HashSet<CubeMeasure>();
      for (String msr : measures) {
        cachedMeasures.add(parent.getMeasureByName(msr));
      }
    }
    return cachedMeasures;
  }

  public Set<CubeDimAttribute> getDimAttributes() {
    if (cachedDims == null) {
      cachedDims = new HashSet<CubeDimAttribute>();
      for (String dim : dimensions) {
        cachedDims.add(parent.getDimAttributeByName(dim));
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
    getProperties().put(MetastoreUtil.getCubeMeasureListKey(getName()), StringUtils.join(measures, ",").toLowerCase());
    getProperties().put(MetastoreUtil.getCubeDimensionListKey(getName()),
        StringUtils.join(dimensions, ",").toLowerCase());
    getProperties().put(MetastoreUtil.getParentCubeNameKey(getName()), parent.getName().toLowerCase());
    getProperties().put(MetastoreUtil.getParentCubeNameKey(getName()), parent.getName().toLowerCase());
  }

  public static Set<String> getMeasures(String name, Map<String, String> props) {
    Set<String> measures = new HashSet<String>();
    String measureStr = props.get(MetastoreUtil.getCubeMeasureListKey(name));
    measures.addAll(Arrays.asList(StringUtils.split(measureStr, ',')));
    return measures;
  }

  public Set<String> getTimedDimensions() {
    String str = getProperties().get(MetastoreUtil.getCubeTimedDimensionListKey(getName()));
    if (str != null) {
      Set<String> timedDimensions = new HashSet<String>();
      timedDimensions.addAll(Arrays.asList(StringUtils.split(str, ',')));
      return timedDimensions;
    } else {
      return parent.getTimedDimensions();
    }
  }

  public static Set<String> getDimensions(String name, Map<String, String> props) {
    Set<String> dimensions = new HashSet<String>();
    String dimStr = props.get(MetastoreUtil.getCubeDimensionListKey(name));
    dimensions.addAll(Arrays.asList(StringUtils.split(dimStr, ',')));
    return dimensions;
  }

  public Cube getParent() {
    return parent;
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
   * @param measure
   * @throws HiveException
   */
  public void addMeasure(String measure) throws HiveException {
    measures.add(measure.toLowerCase());
    getProperties().put(MetastoreUtil.getCubeMeasureListKey(getName()), StringUtils.join(measures, ",").toLowerCase());
  }

  /**
   * Add a new dimension
   * 
   * @param dimension
   * @throws HiveException
   */
  public void addDimension(String dimension) throws HiveException {
    dimensions.add(dimension.toLowerCase());
    getProperties().put(MetastoreUtil.getCubeDimensionListKey(getName()),
        StringUtils.join(dimensions, ",").toLowerCase());
  }

  /**
   * Remove the dimension with name specified
   * 
   * @param dimName
   */
  public void removeDimension(String dimName) {
    dimensions.remove(dimName.toLowerCase());
    getProperties().put(MetastoreUtil.getCubeDimensionListKey(getName()),
        StringUtils.join(dimensions, ",").toLowerCase());
  }

  /**
   * Remove the measure with name specified
   * 
   * @param msrName
   */
  public void removeMeasure(String msrName) {
    measures.remove(msrName.toLowerCase());
    getProperties().put(MetastoreUtil.getCubeMeasureListKey(getName()), StringUtils.join(measures, ",").toLowerCase());
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
    Set<String> dimNames = new HashSet<String>();
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
    Set<String> fieldNames = new HashSet<String>();
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
}
