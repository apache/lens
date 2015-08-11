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

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class ReferencedDimAtrribute extends BaseDimAttribute {
  private static final char CHAIN_REF_COL_SEPARATOR = ',';

  @Getter
  private final List<TableReference> references = new ArrayList<>();
  // boolean whether to say the key is only a denormalized variable kept or can
  // be used in join resolution as well
  @Getter private Boolean isJoinKey = true;
  @Getter private List<ChainRefCol> chainRefColumns = new ArrayList<>();

  @Data
  public static class ChainRefCol {
    private final String chainName;
    private final String refColumn;
  }

  public ReferencedDimAtrribute(FieldSchema column, String displayString, TableReference reference) {
    this(column, displayString, reference, null, null, null);
  }

  public ReferencedDimAtrribute(FieldSchema column, String displayString, TableReference reference, Date startTime,
      Date endTime, Double cost) {
    this(column, displayString, reference, startTime, endTime, cost, true);
  }

  public ReferencedDimAtrribute(FieldSchema column, String displayString, TableReference reference, Date startTime,
      Date endTime, Double cost, boolean isJoinKey) {
    this(column, displayString, reference, startTime, endTime, cost, isJoinKey, null);
  }

  public ReferencedDimAtrribute(FieldSchema column, String displayString, TableReference reference, Date startTime,
      Date endTime, Double cost, boolean isJoinKey, Long numOfDistinctValues) {
    super(column, displayString, startTime, endTime, cost, numOfDistinctValues);
    this.references.add(reference);
    this.isJoinKey = isJoinKey;
  }

  public ReferencedDimAtrribute(FieldSchema column, String displayString, Collection<TableReference> references) {
    this(column, displayString, references, null, null, null);
  }

  public ReferencedDimAtrribute(FieldSchema column, String displayString, Collection<TableReference> references,
      Date startTime, Date endTime, Double cost) {
    this(column, displayString, references, startTime, endTime, cost, true);
  }

  public ReferencedDimAtrribute(FieldSchema column, String displayString, Collection<TableReference> references,
      Date startTime, Date endTime, Double cost, boolean isJoinKey) {
    this(column, displayString, references, startTime, endTime, cost, isJoinKey, null);
  }

  public ReferencedDimAtrribute(FieldSchema column, String displayString, Collection<TableReference> references,
      Date startTime, Date endTime, Double cost, boolean isJoinKey, Long numOfDistinctValues) {
    super(column, displayString, startTime, endTime, cost, numOfDistinctValues);
    this.references.addAll(references);
    this.isJoinKey = isJoinKey;
  }

  public ReferencedDimAtrribute(FieldSchema column, String displayString, String chainName, String refColumn,
      Date startTime, Date endTime, Double cost) {
    this(column, displayString, chainName, refColumn, startTime, endTime, cost, null);
  }

  public ReferencedDimAtrribute(FieldSchema column, String displayString, String chainName, String refColumn,
      Date startTime, Date endTime, Double cost, Long numOfDistinctValues) {
    this(column, displayString,
      Collections.singletonList(new ChainRefCol(chainName.toLowerCase(), refColumn.toLowerCase())), startTime, endTime,
      cost, numOfDistinctValues);
  }

  public ReferencedDimAtrribute(FieldSchema column, String displayString, List<ChainRefCol> chainRefCols,
    Date startTime, Date endTime, Double cost, Long numOfDistinctValues) {
    super(column, displayString, startTime, endTime, cost, numOfDistinctValues);
    chainRefColumns.addAll(chainRefCols);
    this.isJoinKey = false;
  }

  public void addReference(TableReference reference) {
    references.add(reference);
  }

  public boolean removeReference(TableReference ref) {
    return references.remove(ref);
  }

  public boolean useAsJoinKey() {
    return isJoinKey;
  }

  @Override
  public void addProperties(Map<String, String> props) {
    super.addProperties(props);
    if (!chainRefColumns.isEmpty()) {
      StringBuilder chainNamesValue = new StringBuilder();
      StringBuilder refColsValue = new StringBuilder();
      Iterator<ChainRefCol> iter = chainRefColumns.iterator();
      // Add the first without appending separator
      ChainRefCol chainRefCol = iter.next();
      chainNamesValue.append(chainRefCol.getChainName());
      refColsValue.append(chainRefCol.getRefColumn());
      while (iter.hasNext()) {
        chainRefCol = iter.next();
        chainNamesValue.append(CHAIN_REF_COL_SEPARATOR).append(chainRefCol.getChainName());
        refColsValue.append(CHAIN_REF_COL_SEPARATOR).append(chainRefCol.getRefColumn());
      }
      props.put(MetastoreUtil.getDimRefChainNameKey(getName()), chainNamesValue.toString());
      props.put(MetastoreUtil.getDimRefChainColumnKey(getName()), refColsValue.toString());
    } else {
      props.put(MetastoreUtil.getDimensionSrcReferenceKey(getName()),
          MetastoreUtil.getReferencesString(references));
      props.put(MetastoreUtil.getDimUseAsJoinKey(getName()), isJoinKey.toString());
    }
  }

  /**
   * This is used only for serializing
   *
   * @param name
   * @param props
   */
  public ReferencedDimAtrribute(String name, Map<String, String> props) {
    super(name, props);
    String chNamesStr = props.get(MetastoreUtil.getDimRefChainNameKey(getName()));
    if (!StringUtils.isBlank(chNamesStr)) {
      String refColsStr = props.get(MetastoreUtil.getDimRefChainColumnKey(getName()));
      String[] chainNames = StringUtils.split(chNamesStr, ",");
      String[] refCols = StringUtils.split(refColsStr, ",");
      for (int i = 0; i < chainNames.length; i++) {
        chainRefColumns.add(new ChainRefCol(chainNames[i], refCols[i]));
      }
      this.isJoinKey = false;
    } else {
      String refListStr = props.get(MetastoreUtil.getDimensionSrcReferenceKey(getName()));
      String[] refListDims = StringUtils.split(refListStr, ",");
      for (String refDimRaw : refListDims) {
        references.add(new TableReference(refDimRaw));
      }
      String isJoinKeyStr = props.get(MetastoreUtil.getDimUseAsJoinKey(name));
      if (isJoinKeyStr != null) {
        isJoinKey = Boolean.parseBoolean(isJoinKeyStr);
      }
    }
  }

  /**
   * Tells whether the attribute is retrieved from chain
   *
   * @return true/false
   */
  public boolean isChainedColumn() {
    return !chainRefColumns.isEmpty();
  }
}
