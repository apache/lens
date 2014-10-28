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

import java.util.Set;

/**
 * The cube interface on which queries can be accepted
 */
public interface CubeInterface extends Named {

  /**
   * Get all measures of the cube
   * 
   * @return set of {@link CubeMeasure}
   */
  public Set<CubeMeasure> getMeasures();

  /**
   * Get all dimension attributes of the cube
   * 
   * @return set {@link CubeDimAttribute}
   */
  public Set<CubeDimAttribute> getDimAttributes();

  /**
   * Get all expressions defined on the cube
   * 
   * @return set {@link ExprColumn}
   */
  public Set<ExprColumn> getExpressions();

  /**
   * Get dimension attribute given by name
   * 
   * @param dimAttrName
   *          dimension attribute name
   * 
   * @return A {@link CubeDimAttribute} object
   */
  public CubeDimAttribute getDimAttributeByName(String dimAttrName);

  /**
   * Get measure by given by name
   * 
   * @param msrName
   *          Measure name
   * 
   * @return A {@link CubeMeasure} object
   */
  public CubeMeasure getMeasureByName(String msrName);

  /**
   * Get expression by given by name
   * 
   * @param exprName
   *          Expression name
   * 
   * @return A {@link ExprColumn} object
   */
  public ExprColumn getExpressionByName(String exprName);

  /**
   * Get cube column given by column name.
   * 
   * It can be a measure, dimension attribute or an expression.
   * 
   * @param colName
   *          Column name
   * 
   * @return A {@link CubeColumn} object
   */
  public CubeColumn getColumnByName(String colName);

  /**
   * Get all timed dimensions of cube
   * 
   * @return Set of strings
   */
  public Set<String> getTimedDimensions();

  /**
   * Is the cube a derived cube or base cube
   * 
   * @return true if cube is derived, false if it is base
   */
  public boolean isDerivedCube();

  /**
   * Get all measure names
   * 
   * @return Set of strings
   */
  public Set<String> getMeasureNames();

  /**
   * Get all dimension attribute names
   * 
   * @return Set of strings
   */
  public Set<String> getDimAttributeNames();

  /**
   * Get all expression names
   * 
   * @return Set of strings
   */
  public Set<String> getExpressionNames();

  /**
   * Get all field names reachable from cube
   * 
   * @return Set of strings
   */
  public Set<String> getAllFieldNames();

  /**
   * Whether all the fields of cube can be queried.
   * 
   * If false, the fields can queried through derived cubes. Users can look at
   * derived cube fields to know which all fields can be queried together.
   * 
   * If true, all the fields can be directly queried.
   * 
   * @return true or false
   */
  public boolean allFieldsQueriable();
}
