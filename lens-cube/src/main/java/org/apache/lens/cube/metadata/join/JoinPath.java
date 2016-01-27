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
package org.apache.lens.cube.metadata.join;

import java.util.*;

import org.apache.lens.cube.metadata.AbstractCubeTable;

/**
 * A list of table relationships that can be combined to get a join clause
 */
public class JoinPath {
  final List<TableRelationship> edges;
  // Store the map of a table against all columns of that table which are in the path
  private Map<AbstractCubeTable, List<String>> columnsForTable = new HashMap<>();

  public JoinPath() {
    edges = new ArrayList<>();
  }

  public JoinPath(JoinPath other) {
    edges = new ArrayList<>(other.edges);
  }

  public void initColumnsForTable() {
    if (!columnsForTable.isEmpty()) {
      // already initialized
      return;
    }
    for (TableRelationship edge : edges) {
      addColumnsForEdge(edge);
    }
  }

  public void addEdge(TableRelationship edge) {
    edges.add(edge);
  }

  public boolean isEmpty() {
    return edges.isEmpty();
  }

  public List<TableRelationship> getEdges() {
    return edges;
  }

  private void addColumnsForEdge(TableRelationship edge) {
    addColumn(edge.getFromTable(), edge.getFromColumn());
    addColumn(edge.getToTable(), edge.getToColumn());
  }

  private void addColumn(AbstractCubeTable table, String column) {
    if (table == null || column == null) {
      return;
    }
    List<String> columns = columnsForTable.get(table);
    if (columns == null) {
      columns = new ArrayList<>();
      columnsForTable.put(table, columns);
    }
    columns.add(column);
  }

  public List<String> getColumnsForTable(AbstractCubeTable table) {
    return columnsForTable.get(table);
  }

  public Set<AbstractCubeTable> getAllTables() {
    return columnsForTable.keySet();
  }

  public boolean containsColumnOfTable(String column, AbstractCubeTable table) {
    for (TableRelationship edge : edges) {
      if ((table.equals(edge.getFromTable()) && column.equals(edge.getFromColumn()))
        || table.equals(edge.getToTable()) && column.equals(edge.getToColumn())) {
        return true;
      }
    }
    return false;
  }

  public String toString() {
    return edges.toString();
  }
}
