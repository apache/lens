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
package org.apache.lens.cli.table;


import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlType;

import org.apache.lens.api.metastore.XField;
import org.apache.lens.api.metastore.XFlattenedColumn;
import org.apache.lens.api.metastore.XFlattenedColumns;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class XFlattenedColumnTable {
  private final String table;
  Map<Class<? extends XField>, CollectionTable<XFlattenedColumn>> tables = Maps.newLinkedHashMap();
  List<String> chainNames = Lists.newArrayList();
  List<String> tableNames = Lists.newArrayList();

  public XFlattenedColumnTable(XFlattenedColumns flattenedColumns, String table) {
    this.table = table;
    for (XFlattenedColumn column : flattenedColumns.getFlattenedColumn()) {
      XField field = firstNonNull(column.getDimAttribute(), column.getMeasure(), column.getExpression());
      if (field != null) {
        if (!tables.containsKey(field.getClass())) {
          tables.put(field.getClass(), CollectionTableFactory.getCollectionTable(field.getClass(), table));
        }
        tables.get(field.getClass()).getCollection().add(column);
      } else {
        if (column.getChainName() != null) {
          chainNames.add(column.getChainName());
        }
        if (column.getTableName() != null) {
          tableNames.add(column.getTableName());
        }
      }
    }
  }

  public static <T> T firstNonNull(T... args) {
    for (T arg : args) {
      if (arg != null) {
        return arg;
      }
    }
    return null;
  }


  @Override
  public String toString() {
    String sep = "=============================";
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<Class<? extends XField>, CollectionTable<XFlattenedColumn>> entry : tables.entrySet()) {
      String title =
        entry.getKey().getAnnotation(XmlType.class).name().replaceAll("^x_", "").replaceAll("_", " ") + "s";
      sb.append(title).append("\n").append(sep).append("\n").append(entry.getValue()).append("\n");
    }
    String sep1 = "";
    if (!chainNames.isEmpty()) {
      sb.append("Accessible Join Chains\n").append(sep).append("\n");
      for (String chain : chainNames) {
        sb.append(sep1).append(chain);
        sep1 = "\n";
      }
    }
    sep1 = "";
    if (!tableNames.isEmpty()) {
      sb.append("Accessible Tables\n").append(sep).append("\n");
      for (String table : tableNames) {
        sb.append(sep1).append(table);
        sep1 = "\n";
      }
    }
    return sb.toString();
  }
}
