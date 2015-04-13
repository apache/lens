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

import java.util.Date;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import static com.google.common.base.Preconditions.*;

import com.google.common.base.Optional;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.apachecommons.CommonsLog;

@CommonsLog @ToString(callSuper=true, includeFieldNames=true)
public class BaseDimAttribute extends CubeDimAttribute {
  @Getter private final String type;
  @Getter private Optional<Long> numOfDistinctValues = Optional.absent();

  public BaseDimAttribute(FieldSchema column) {
    this(column, null, null, null, null);
  }

  public BaseDimAttribute(FieldSchema column, String displayString, Date startTime, Date endTime, Double cost) {
    this(column, displayString, startTime, endTime, cost, null);
  }

  public BaseDimAttribute(FieldSchema column, String displayString, Date startTime, Date endTime, Double cost,
      Long numOfDistinctValues) {
    super(column.getName(), column.getComment(), displayString, startTime, endTime, cost);
    this.type = column.getType();
    checkNotNull(type);
    Optional<Long> optionalNumOfDistnctValues = Optional.fromNullable(numOfDistinctValues);
    if (optionalNumOfDistnctValues.isPresent()) {
      this.numOfDistinctValues = optionalNumOfDistnctValues;
      checkArgument(this.numOfDistinctValues.get() > 0);
    }
  }

  @Override
  public void addProperties(Map<String, String> props) {
    super.addProperties(props);
    props.put(MetastoreUtil.getDimTypePropertyKey(getName()), type);
    if (numOfDistinctValues.isPresent()) {
      props.put(MetastoreUtil.getDimNumOfDistinctValuesPropertyKey(getName()),
          String.valueOf(numOfDistinctValues.get()));
    }
  }

  /**
   * This is used only for serializing
   *
   * @param name
   * @param props
   */
  public BaseDimAttribute(String name, Map<String, String> props) {
    super(name, props);
    this.type = getDimType(name, props);
    this.numOfDistinctValues = getDimNumOfDistinctValues(name, props);
  }

  public static String getDimType(String name, Map<String, String> props) {
    return props.get(MetastoreUtil.getDimTypePropertyKey(name));
  }

  public static Optional<Long> getDimNumOfDistinctValues(String name, Map<String, String> props) {
    if (props.containsKey(MetastoreUtil.getDimNumOfDistinctValuesPropertyKey(name))) {
      try {
        return Optional.of(Long.parseLong((props.get(MetastoreUtil.getDimNumOfDistinctValuesPropertyKey(name)))));
      } catch (NumberFormatException ne) {
        log.error("NumberFormat exception while parsing the num of distinct vlaues "
            + props.get(MetastoreUtil.getDimNumOfDistinctValuesPropertyKey(name)));
      }
    }
    return Optional.absent();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((getType() == null) ? 0 : getType().toLowerCase().hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    BaseDimAttribute other = (BaseDimAttribute) obj;
    if (this.getType() == null) {
      if (other.getType() != null) {
        return false;
      }
    } else if (!this.getType().equalsIgnoreCase(other.getType())) {
      return false;
    }
    return true;
  }
}
