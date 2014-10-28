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
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

public class ReferencedDimAtrribute extends BaseDimAttribute {
  private final List<TableReference> references = new ArrayList<TableReference>();
  // boolean whether to say the key is only a denormalized variable kept or can
  // be used in join resolution as well
  private Boolean isJoinKey = true;

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

  public void addReference(TableReference reference) {
    references.add(reference);
  }

  public List<TableReference> getReferences() {
    return references;
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
    props
        .put(MetastoreUtil.getDimensionSrcReferenceKey(getName()), MetastoreUtil.getDimensionDestReference(references));
    props.put(MetastoreUtil.getDimUseAsJoinKey(getName()), isJoinKey.toString());
  }

  /**
   * This is used only for serializing
   * 
   * @param name
   * @param props
   */
  public ReferencedDimAtrribute(String name, Map<String, String> props) {
    super(name, props);
    String refListStr = props.get(MetastoreUtil.getDimensionSrcReferenceKey(getName()));
    String refListDims[] = StringUtils.split(refListStr, ",");
    for (String refDimRaw : refListDims) {
      references.add(new TableReference(refDimRaw));
    }
    String isJoinKeyStr = props.get(MetastoreUtil.getDimUseAsJoinKey(name));
    if (isJoinKeyStr != null) {
      isJoinKey = Boolean.parseBoolean(isJoinKeyStr);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((getReferences() == null) ? 0 : getReferences().hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    ReferencedDimAtrribute other = (ReferencedDimAtrribute) obj;
    if (this.getReferences() == null) {
      if (other.getReferences() != null) {
        return false;
      }
    } else if (!this.getReferences().equals(other.getReferences())) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    String str = super.toString();
    str += "references:" + getReferences();
    return str;
  }
}
