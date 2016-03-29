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

import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractCubeTable implements Named {
  private final String name;
  private final List<FieldSchema> columns;
  private final Map<String, String> properties = new HashMap<>();
  private double weight;

  protected AbstractCubeTable(String name, List<FieldSchema> columns, Map<String, String> props, double weight) {
    this.name = name.toLowerCase();
    this.columns = columns;
    this.weight = weight;
    if (props != null) {
      this.properties.putAll(props);
    }
  }

  protected AbstractCubeTable(Table hiveTable) {
    this.name = hiveTable.getTableName().toLowerCase();
    this.columns = hiveTable.getCols();
    this.properties.putAll(hiveTable.getParameters());
    this.weight = getWeight(getProperties(), getName());
  }

  public abstract CubeTableType getTableType();

  public abstract Set<String> getStorages();

  public Map<String, String> getProperties() {
    return properties;
  }

  public static double getWeight(Map<String, String> properties, String name) {
    String wtStr = properties.get(MetastoreUtil.getCubeTableWeightKey(name));
    return wtStr == null ? 0L : Double.parseDouble(wtStr);
  }

  protected void addProperties() {
    properties.put(MetastoreConstants.TABLE_TYPE_KEY, getTableType().name());
    properties.put(MetastoreUtil.getCubeTableWeightKey(name), String.valueOf(weight));
  }

  public String getName() {
    return name;
  }

  public List<FieldSchema> getColumns() {
    return columns;
  }

  public double weight() {
    return weight;
  }

  /**
   * Alters the weight of table
   *
   * @param weight Weight of the table.
   */
  public void alterWeight(double weight) {
    this.weight = weight;
    this.addProperties();
  }

  /**
   * Add more table properties
   *
   * @param props  properties
   */
  public void addProperties(Map<String, String> props) {
    this.properties.putAll(props);
    addProperties();
  }

  /**
   * Remove property specified by the key
   *
   * @param propKey property key
   */
  public void removeProperty(String propKey) {
    properties.remove(propKey);
  }

  /**
   * Alters the column if already existing or just adds it if it is new column
   *
   * @param column The column spec as FieldSchema - name, type and a comment
   */
  protected void alterColumn(@NonNull FieldSchema column) {
    Iterator<FieldSchema> columnItr = columns.iterator();
    int alterPos = -1;
    int i = 0;
    FieldSchema toReplace = null;
    while (columnItr.hasNext()) {
      FieldSchema c = columnItr.next();
      // Replace column if already existing
      if (column.getName().equalsIgnoreCase(c.getName())) {
        toReplace = c;
        columnItr.remove();
        alterPos = i;
      }
      i++;
    }
    if (alterPos != -1) {
      log.info("In {} replacing column {}:{} to {}:{}", getName(), toReplace.getName(), toReplace.getType(),
        column.getName(), column.getType());
      columns.add(alterPos, column);
    } else {
      columns.add(column);
    }
  }

  /**
   * Adds or alters the columns passed
   *
   * @param columns The collection of columns
   */
  protected void addColumns(@NonNull Collection<FieldSchema> columns) {
    for (FieldSchema column : columns) {
      alterColumn(column);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    AbstractCubeTable other = (AbstractCubeTable) obj;

    if (!this.getName().equalsIgnoreCase(other.getName())) {
      return false;
    }
    if (this.getColumns() == null) {
      if (other.getColumns() != null) {
        return false;
      }
    } else {
      if (!this.getColumns().equals(other.getColumns())) {
        return false;
      }
    }
    if (this.weight() != other.weight()) {
      return false;
    }
    return true;
  }

  public Date getDateFromProperty(String propKey, boolean relative, boolean start) {
    String prop = getProperties().get(propKey);
    try {
      if (StringUtils.isNotBlank(prop)) {
        if (relative) {
          return DateUtil.resolveRelativeDate(prop, now());
        } else {
          return DateUtil.resolveAbsoluteDate(prop);
        }
      }
    } catch (LensException e) {
      log.error("unable to parse {} {} date: {}", relative ? "relative" : "absolute", start ? "start" : "end", prop);
    }
    return start ? DateUtil.MIN_DATE : DateUtil.MAX_DATE;
  }


  @Override
  public String toString() {
    return getName();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((getName() == null) ? 0 : getName().hashCode());
    return result;
  }

  public Set<String> getAllFieldNames() {
    List<FieldSchema> fields = getColumns();
    Set<String> columns = new HashSet<>(fields.size());
    for (FieldSchema f : fields) {
      columns.add(f.getName().toLowerCase());
    }
    return columns;
  }

  public Date now() {
    return new Date();
  }

}
