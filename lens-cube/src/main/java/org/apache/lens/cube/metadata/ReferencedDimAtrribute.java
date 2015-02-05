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

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@EqualsAndHashCode(callSuper = true)
@ToString
public class ReferencedDimAtrribute extends BaseDimAttribute {
  @Getter
  private final List<TableReference> references = new ArrayList<TableReference>();
  // boolean whether to say the key is only a denormalized variable kept or can
  // be used in join resolution as well
  private Boolean isJoinKey = true;
  @Getter
  private String chainName = null;
  @Getter
  private String refColumn = null;

  public ReferencedDimAtrribute(FieldSchema column, String displayString, TableReference reference) {
    this(column, displayString, reference, null, null, null);
  }

  public ReferencedDimAtrribute(FieldSchema column, String displayString, TableReference reference, Date startTime,
    Date endTime, Double cost) {
    this(column, displayString, reference, startTime, endTime, cost, true);
  }

  public ReferencedDimAtrribute(FieldSchema column, String displayString, TableReference reference, Date startTime,
    Date endTime, Double cost, boolean isJoinKey) {
    super(column, displayString, startTime, endTime, cost);
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
    super(column, displayString, startTime, endTime, cost);
    this.references.addAll(references);
    this.isJoinKey = isJoinKey;
  }

  public ReferencedDimAtrribute(FieldSchema column, String displayString, String chainName, String refColumn,
    Date startTime, Date endTime, Double cost) {
    super(column, displayString, startTime, endTime, cost);
    this.chainName = chainName.toLowerCase();
    this.refColumn = refColumn.toLowerCase();
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
    if (chainName != null) {
      props.put(MetastoreUtil.getDimRefChainNameKey(getName()), chainName);
      props.put(MetastoreUtil.getDimRefChainColumnKey(getName()), refColumn);
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
    String chName = props.get(MetastoreUtil.getDimRefChainNameKey(getName()));
    if (!StringUtils.isBlank(chName)) {
      this.chainName = chName;
      this.refColumn = props.get(MetastoreUtil.getDimRefChainColumnKey(getName()));
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
    return chainName != null;
  }
}
