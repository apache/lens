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

import java.text.DateFormat;
import java.util.*;

import org.apache.lens.server.api.error.LensException;

import com.google.common.collect.ImmutableMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@EqualsAndHashCode
public class FactPartition implements Comparable<FactPartition> {
  @Getter
  private final String partCol;
  @Getter
  private final Date partSpec;
  @Getter
  private final Set<String> storageTables = new LinkedHashSet<String>();
  @Getter
  private final UpdatePeriod period;
  @Getter
  @Setter
  private FactPartition containingPart;
  @Getter
  private final DateFormat partFormat;
  @Getter
  @Setter
  private boolean found = false;

  public FactPartition(String partCol, Date partSpec, UpdatePeriod period,
    FactPartition containingPart, DateFormat partFormat) {
    this.partCol = partCol;
    this.partSpec = partSpec;
    this.period = period;
    this.containingPart = containingPart;
    this.partFormat = partFormat;
  }

  public FactPartition(String partCol, Date partSpec, UpdatePeriod period, FactPartition containingPart,
    DateFormat partFormat, Set<String> storageTables) {
    this(partCol, partSpec, period, containingPart, partFormat);
    if (storageTables != null) {
      this.storageTables.addAll(storageTables);
    }
  }

  public FactPartition(String partCol, TimePartition timePartition) {
    this(partCol, timePartition, null, null);
  }

  public FactPartition(String partCol, TimePartition timePartition, FactPartition containingPart, Set<String>
    storageTables) {
    this(partCol, timePartition.getDate(), timePartition.getUpdatePeriod(), containingPart, null, storageTables);
  }

  public boolean hasContainingPart() {
    return containingPart != null;
  }

  public String getFormattedPartSpec() {
    if (partFormat == null) {
      return getPartString();
    } else {
      return partFormat.format(partSpec);
    }
  }

  public String getPartString() {
    return period.format(partSpec);
  }

  public String getFormattedFilter(String tableName) {
    return getFormattedFilter(partCol, tableName);
  }

  public String getFormattedFilter(String partCol, String tableName) {
    StringBuilder builder = new StringBuilder();
    if (containingPart != null) {
      builder.append(containingPart.getFormattedFilter(tableName));
      builder.append(" AND ");
    }
    if (tableName != null) {
      builder.append(tableName);
      builder.append(".");
    }
    builder.append(partCol);
    builder.append(" = '").append(getFormattedPartSpec()).append("'");
    return builder.toString();
  }

  public String getFilter() {
    StringBuilder builder = new StringBuilder();
    if (containingPart != null) {
      builder.append(containingPart.getFilter());
      builder.append(" AND ");
    }
    builder.append(partCol);
    builder.append(" = '").append(getPartString()).append("'");
    return builder.toString();
  }

  @Override
  public String toString() {
    return getFilter();
  }

  public int compareTo(FactPartition o) {
    int colComp = this.partCol.compareTo(o.partCol);
    if (colComp == 0) {
      int partComp = 0;
      if (this.partSpec != null) {
        if (o.partSpec == null) {
          partComp = 1;
        } else {
          partComp = this.partSpec.compareTo(o.partSpec);
        }
      } else {
        if (o.partSpec != null) {
          partComp = -1;
        } else {
          partComp = 0;
        }
      }
      if (partComp == 0) {
        int upComp = 0;
        if (this.period != null && o.period != null) {
          upComp = this.period.compareTo(o.period);
        } else if (this.period == null && o.period == null) {
          upComp = 0;
        } else if (this.period == null) {
          upComp = -1;
        } else {
          upComp = 1;
        }
        if (upComp == 0) {
          if (this.containingPart != null) {
            if (o.containingPart == null) {
              return 1;
            }
            return this.containingPart.compareTo(o.containingPart);
          } else {
            if (o.containingPart != null) {
              return -1;
            } else {
              return 0;
            }
          }
        }
        return upComp;
      }
      return partComp;
    }
    return colComp;
  }

  public TimePartition getTimePartition() throws LensException {
    return TimePartition.of(getPeriod(), getPartSpec());
  }

  public double getAllTableWeights(ImmutableMap<String, Double> tableWeights) {
    double weight = 0;
    Map<String, Double> tblWithoutDBWeghts = new HashMap<>();
    for (Map.Entry<String, Double> entry : tableWeights.entrySet()) {
      tblWithoutDBWeghts.put(entry.getKey().substring(entry.getKey().indexOf('.') + 1), entry.getValue());
    }
    for (String tblName : getStorageTables()) {
      Double tblWeight = tblWithoutDBWeghts.get(tblName);
      if (tblWeight != null) {
        weight += tblWeight;
      }
    }
    return weight;
  }
}
