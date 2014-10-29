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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.log4j.Logger;

public abstract class AbstractCubeTable implements Named {
  public static final Logger LOG = Logger.getLogger(AbstractCubeTable.class);
  private final String name;
  private final List<FieldSchema> columns;
  private final Map<String, String> properties = new HashMap<String, String>();
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
   * @param weight
   */
  public void alterWeight(double weight) {
    this.weight = weight;
    this.addProperties();
  }

  /**
   * Add more table properties
   * 
   * @param properties
   */
  public void addProperties(Map<String, String> props) {
    this.properties.putAll(props);
    addProperties();
  }

  /**
   * Remove property specified by the key
   * 
   * @param propKey
   */
  public void removeProperty(String propKey) {
    properties.remove(propKey);
  }

  /**
   * Alters the column if already existing or just adds it if it is new column
   * 
   * @param column
   * @throws HiveException
   */
  protected void alterColumn(FieldSchema column) throws HiveException {
    if (column == null) {
      throw new HiveException("Column cannot be null");
    }
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
      LOG.info("In " + getName() + " replacing column " + toReplace.getName() + ":" + toReplace.getType() + " to "
          + column.getName() + ":" + column.getType());
      columns.add(alterPos, column);
    } else {
      columns.add(column);
    }
  }

  /**
   * Adds or alters the columns passed
   * 
   * @param columns
   * @throws HiveException
   */
  protected void addColumns(Collection<FieldSchema> columns) throws HiveException {
    if (columns == null) {
      throw new HiveException("Columns cannot be null");
    }
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
    Set<String> columns = new HashSet<String>(fields.size());
    for (FieldSchema f : fields) {
      columns.add(f.getName().toLowerCase());
    }
    return columns;
  }
}
