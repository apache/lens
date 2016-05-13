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

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class ReferencedDimAttribute extends BaseDimAttribute {
  private static final char CHAIN_REF_COL_SEPARATOR = ',';
  @Getter
  private List<ChainRefCol> chainRefColumns = new ArrayList<>();

  @Data
  public static class ChainRefCol {
    private final String chainName;
    private final String refColumn;

    public ChainRefCol(String chainName, String refColumn) {
      this.chainName = chainName.toLowerCase();
      this.refColumn = refColumn.toLowerCase();
    }
  }

  public ReferencedDimAttribute(FieldSchema column, String displayString, String chainName, String refColumn,
    Date startTime, Date endTime, Double cost) throws LensException {
    this(column, displayString, chainName, refColumn, startTime, endTime, cost, null);
  }

  public ReferencedDimAttribute(FieldSchema column, String displayString, String chainName, String refColumn,
    Date startTime, Date endTime, Double cost, Long numOfDistinctValues) throws LensException {
    this(column, displayString,
      Collections.singletonList(new ChainRefCol(chainName, refColumn)), startTime, endTime,
      cost, numOfDistinctValues);
  }

  public ReferencedDimAttribute(FieldSchema column, String displayString, List<ChainRefCol> chainRefCols,
    Date startTime, Date endTime, Double cost, Long numOfDistinctValues) throws LensException {
    this(column, displayString, chainRefCols, startTime, endTime, cost, numOfDistinctValues,
        null, new HashMap<String, String>());
  }

  public ReferencedDimAttribute(FieldSchema column, String displayString, List<ChainRefCol> chainRefCols,
    Date startTime, Date endTime, Double cost, Long numOfDistinctValues, List<String> values, Map<String, String> tags)
    throws LensException {
    super(column, displayString, startTime, endTime, cost, numOfDistinctValues, values, tags);
    if (chainRefCols.isEmpty()) {
      throw new LensException(LensCubeErrorCode.ERROR_IN_ENTITY_DEFINITION.getLensErrorInfo(), " Ref column: "
        + getName() + " does not have any chain_ref_column defined");
    }
    chainRefColumns.addAll(chainRefCols);
  }

  @Override
  public void addProperties(Map<String, String> props) {
    super.addProperties(props);
    StringBuilder chainNamesValue = new StringBuilder();
    StringBuilder refColsValue = new StringBuilder();
    Iterator<ChainRefCol> iterator = chainRefColumns.iterator();
    // Add the first without appending separator
    ChainRefCol chainRefCol = iterator.next();
    chainNamesValue.append(chainRefCol.getChainName());
    refColsValue.append(chainRefCol.getRefColumn());
    while (iterator.hasNext()) {
      chainRefCol = iterator.next();
      chainNamesValue.append(CHAIN_REF_COL_SEPARATOR).append(chainRefCol.getChainName());
      refColsValue.append(CHAIN_REF_COL_SEPARATOR).append(chainRefCol.getRefColumn());
    }
    props.put(MetastoreUtil.getDimRefChainNameKey(getName()), chainNamesValue.toString());
    props.put(MetastoreUtil.getDimRefChainColumnKey(getName()), refColsValue.toString());
  }

  /**
   * This is used only for serializing
   *
   * @param name attribute name
   * @param props Properties
   */
  public ReferencedDimAttribute(String name, Map<String, String> props) throws LensException {
    super(name, props);
    String chNamesStr = props.get(MetastoreUtil.getDimRefChainNameKey(getName()));
    if (!StringUtils.isBlank(chNamesStr)) {
      String refColsStr = props.get(MetastoreUtil.getDimRefChainColumnKey(getName()));
      String[] chainNames = StringUtils.split(chNamesStr, ",");
      String[] refCols = StringUtils.split(refColsStr, ",");
      for (int i = 0; i < chainNames.length; i++) {
        chainRefColumns.add(new ChainRefCol(chainNames[i], refCols[i]));
      }
    } else {
      throw new LensException(LensCubeErrorCode.ERROR_IN_ENTITY_DEFINITION.getLensErrorInfo(), " Ref column: "
        + getName() + " does not have any chain_ref_column defined");
    }
  }

}
