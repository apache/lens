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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

public abstract class CubeMeasure extends CubeColumn {
  private final String formatString;
  private final String aggregate;
  private final String unit;
  private final FieldSchema column;
  private final Double min;
  private final Double max;

  protected CubeMeasure(FieldSchema column, String displayString, String formatString, String aggregate, String unit,
                        Date startTime, Date endTime, Double cost, Double min, Double max) {
    this(column, displayString, formatString, aggregate, unit,
        startTime, endTime, cost, min, max, new HashMap<String, String>());
  }

  protected CubeMeasure(FieldSchema column, String displayString, String formatString, String aggregate, String unit,
    Date startTime, Date endTime, Double cost, Double min, Double max, Map<String, String> tags) {
    super(column.getName(), column.getComment(), displayString, startTime, endTime, cost, tags);
    this.column = column;
    assert (column != null);
    assert (column.getName() != null);
    assert (column.getType() != null);
    this.formatString = formatString;
    this.aggregate = aggregate;
    this.unit = unit;
    this.min = min;
    this.max = max;
  }

  protected CubeMeasure(String name, Map<String, String> props) {
    super(name, props);
    this.column = new FieldSchema(name, props.get(MetastoreUtil.getMeasureTypePropertyKey(name)), "");
    this.formatString = props.get(MetastoreUtil.getMeasureFormatPropertyKey(name));
    this.aggregate = props.get(MetastoreUtil.getMeasureAggrPropertyKey(name));
    this.unit = props.get(MetastoreUtil.getMeasureUnitPropertyKey(name));
    String minStr = props.get(MetastoreUtil.getMeasureMinPropertyKey(name));
    String maxStr = props.get(MetastoreUtil.getMeasureMaxPropertyKey(name));
    if (minStr != null && maxStr != null && !minStr.isEmpty() && !maxStr.isEmpty()) {
      this.min = Double.parseDouble(minStr);
      this.max = Double.parseDouble(maxStr);
    } else {
      this.min = null;
      this.max = null;
    }
  }

  public String getFormatString() {
    return formatString;
  }

  public String getAggregate() {
    return aggregate;
  }

  public String getUnit() {
    return unit;
  }

  public FieldSchema getColumn() {
    return column;
  }

  public Double getMin() {
    return min;
  }

  public Double getMax() {
    return max;
  }

  @Override
  public String getName() {
    return column.getName();
  }

  public String getType() {
    return column.getType();
  }

  @Override
  public String toString() {
    String str = super.toString() + ":" + getType();
    if (unit != null) {
      str += ",unit:" + unit;
    }
    if (aggregate != null) {
      str += ",aggregate:" + aggregate;
    }
    if (formatString != null) {
      str += ",formatString:" + formatString;
    }
    return str;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    CubeMeasure other = (CubeMeasure) obj;
    if (!this.getName().equalsIgnoreCase(other.getName())) {
      return false;
    }
    if (!this.getType().equalsIgnoreCase(other.getType())) {
      return false;
    }
    if (this.getUnit() == null) {
      if (other.getUnit() != null) {
        return false;
      }
    } else if (!this.getUnit().equalsIgnoreCase(other.getUnit())) {
      return false;
    }
    if (this.getAggregate() == null) {
      if (other.getAggregate() != null) {
        return false;
      }
    } else if (!this.getAggregate().equalsIgnoreCase(other.getAggregate())) {
      return false;
    }
    if (this.getFormatString() == null) {
      if (other.getFormatString() != null) {
        return false;
      }
    } else if (!this.getFormatString().equalsIgnoreCase(other.getFormatString())) {
      return false;
    }
    if (this.getMin() == null) {
      if (other.getMin() != null) {
        return false;
      }
    } else if (!this.getMin().equals(other.getMin())) {
      return false;
    }
    if (this.getMax() == null) {
      if (other.getMax() != null) {
        return false;
      }
    } else if (!this.getMax().equals(other.getMax())) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((getType() == null) ? 0 : getType().toLowerCase().hashCode());
    result = prime * result + ((unit == null) ? 0 : unit.toLowerCase().hashCode());
    result = prime * result + ((aggregate == null) ? 0 : aggregate.toLowerCase().hashCode());
    result = prime * result + ((formatString == null) ? 0 : formatString.toLowerCase().hashCode());
    return result;
  }

  @Override
  public void addProperties(Map<String, String> props) {
    super.addProperties(props);
    props.put(MetastoreUtil.getMeasureClassPropertyKey(getName()), getClass().getName());
    props.put(MetastoreUtil.getMeasureTypePropertyKey(getName()), getType());
    if (unit != null) {
      props.put(MetastoreUtil.getMeasureUnitPropertyKey(getName()), unit);
    }
    if (getFormatString() != null) {
      props.put(MetastoreUtil.getMeasureFormatPropertyKey(getName()), formatString);
    }
    if (aggregate != null) {
      props.put(MetastoreUtil.getMeasureAggrPropertyKey(getName()), aggregate);
    }
    if (min != null) {
      props.put(MetastoreUtil.getMeasureMinPropertyKey(getName()), min.toString());
    }
    if (max != null) {
      props.put(MetastoreUtil.getMeasureMaxPropertyKey(getName()), max.toString());
    }
  }
}
