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

public final class ColumnMeasure extends CubeMeasure {

  public ColumnMeasure(FieldSchema column, String displayString, String formatString, String aggregate, String unit) {
    this(column, displayString, formatString, aggregate, unit, null, null, null, null, null, null);
  }

  public ColumnMeasure(FieldSchema column) {
    this(column, null, null, null, null, null, null, null, null, null, null);
  }

  public ColumnMeasure(FieldSchema column, String displayString, String formatString, String aggregate, String unit,
    Date startTime, Date endTime, Double cost) {
    super(column, displayString, formatString, aggregate, unit, startTime, endTime, cost, null, null);
  }

  public ColumnMeasure(FieldSchema column, String displayString, String formatString, String aggregate, String unit,
                       Date startTime, Date endTime, Double cost, Double min, Double max) {
    this(column, displayString, formatString, aggregate, unit, startTime,
        endTime, cost, min, max, new HashMap<String, String>());
  }

  public ColumnMeasure(FieldSchema column, String displayString, String formatString, String aggregate, String unit,
    Date startTime, Date endTime, Double cost, Double min, Double max, Map<String, String> tags) {
    super(column, displayString, formatString, aggregate, unit, startTime, endTime, cost, min, max, tags);
  }

  public ColumnMeasure(String name, Map<String, String> props) {
    super(name, props);
  }

  @Override
  public void addProperties(Map<String, String> props) {
    super.addProperties(props);
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
    return true;
  }
}
