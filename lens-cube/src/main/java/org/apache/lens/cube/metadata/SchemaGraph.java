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

import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.util.*;

public class SchemaGraph {
  /*
   * An edge in the schema graph
   */
  public static class TableRelationship {
    final String fromColumn;
    final AbstractCubeTable fromTable;
    final String toColumn;
    final AbstractCubeTable toTable;

    public TableRelationship(String fromCol, AbstractCubeTable fromTab, String toCol, AbstractCubeTable toTab) {
      fromColumn = fromCol;
      fromTable = fromTab;
      toColumn = toCol;
      toTable = toTab;
    }

    public String getFromColumn() {
      return fromColumn;
    }

    public String getToColumn() {
      return toColumn;
    }

    public AbstractCubeTable getFromTable() {
      return fromTable;
    }

    public AbstractCubeTable getToTable() {
      return toTable;
    }

    @Override
    public String toString() {
      return fromTable.getName() + "." + fromColumn + "->" + toTable.getName() + "." + toColumn;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof TableRelationship)) {
        return false;
      }

      TableRelationship other = (TableRelationship) obj;

      return fromColumn.equals(other.fromColumn) && toColumn.equals(other.toColumn)
          && fromTable.equals(other.fromTable) && toTable.equals(other.toTable);
    }

    @Override
    public int hashCode() {
      return toString().hashCode();
    }
  }

  /**
   * A list of table relationships that can be combined to get a join clause
   */
  public static class JoinPath {
    final List<TableRelationship> edges;
    // Store the map of a table against all columns of that table which are in
    // the path
    private Map<AbstractCubeTable, List<String>> columnsForTable = new HashMap<AbstractCubeTable, List<String>>();

    public JoinPath() {
      edges = new ArrayList<TableRelationship>();
    }

    public JoinPath(JoinPath other) {
      edges = new ArrayList<TableRelationship>(other.edges);
    }

    public void initColumnsForTable() {
      if (!columnsForTable.isEmpty()) {
        // already inited
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
        columns = new ArrayList<String>();
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

  /**
   * Perform a search for join paths on the schema graph
   */
  public static class GraphSearch {
    private final AbstractCubeTable source;
    private final AbstractCubeTable target;
    private final List<List<TableRelationship>> paths;
    private final Map<AbstractCubeTable, Set<TableRelationship>> graph;
    // Used in tests to validate that all paths are searched
    private boolean trimLongerPaths = true;

    public GraphSearch(AbstractCubeTable source, AbstractCubeTable target, SchemaGraph graph) {
      this.source = source;
      this.target = target;
      this.paths = new ArrayList<List<TableRelationship>>();

      if (target instanceof CubeInterface) {
        this.graph = graph.getCubeGraph((CubeInterface) target);
      } else if (target instanceof Dimension) {
        this.graph = graph.getDimOnlyGraph();
      } else {
        throw new IllegalArgumentException("Target neither cube nor dimension");
      }
    }

    public List<JoinPath> findAllPathsToTarget() {
      List<JoinPath> allPaths = findAllPathsToTarget(source, new JoinPath(), new HashSet<AbstractCubeTable>());
      // Retain only the smallest paths
      if (trimLongerPaths && allPaths != null && !allPaths.isEmpty()) {
        JoinPath smallestPath = Collections.min(allPaths, new Comparator<JoinPath>() {
          @Override
          public int compare(JoinPath joinPath, JoinPath joinPath2) {
            return joinPath.getEdges().size() - joinPath2.getEdges().size();
          }
        });

        Iterator<JoinPath> itr = allPaths.iterator();
        while (itr.hasNext()) {
          if (itr.next().getEdges().size() > smallestPath.getEdges().size()) {
            itr.remove();
          }
        }
      }
      return allPaths;
    }

    public void setTrimLongerPaths(boolean val) {
      trimLongerPaths = val;
    }

    /**
     * Recursive DFS to get all paths between source and target. Let path till
     * this node = p Paths at node adjacent to target = [edges leading to
     * target] Path at a random node = [path till this node + p for each p in
     * path(neighbors)]
     */
    List<JoinPath> findAllPathsToTarget(AbstractCubeTable source, JoinPath joinPathTillSource,
        Set<AbstractCubeTable> visited) {
      List<JoinPath> joinPaths = new ArrayList<JoinPath>();
      visited.add(source);

      if (graph.get(source) == null) {
        return joinPaths;
      }
      for (TableRelationship edge : graph.get(source)) {
        if (visited.contains(edge.getFromTable())) {
          continue;
        }

        JoinPath p = new JoinPath(joinPathTillSource);
        p.addEdge(edge);

        if (edge.getFromTable().equals(target)) {
          // Got a direct path
          joinPaths.add(p);
        } else if (edge.getFromTable() instanceof Dimension) {
          List<JoinPath> pathsFromNeighbor = findAllPathsToTarget(edge.getFromTable(), new JoinPath(p), visited);
          for (JoinPath pn : pathsFromNeighbor) {
            if (!pn.isEmpty())
              joinPaths.add(pn);
          }
        }
      }

      return joinPaths;
    }
  }

  /**
   * Graph of tables in the cube metastore. Links between the tables are
   * relationships in the cube.
   */
  private final CubeMetastoreClient metastore;
  // Graph for each cube
  private Map<CubeInterface, Map<AbstractCubeTable, Set<TableRelationship>>> cubeToGraph;

  // sub graph that contains only dimensions, mainly used while checking
  // connectivity
  // between a set of dimensions
  private Map<AbstractCubeTable, Set<TableRelationship>> dimOnlySubGraph;

  public SchemaGraph(CubeMetastoreClient metastore) throws HiveException {
    this.metastore = metastore;
    buildSchemaGraph();
  }

  public Map<AbstractCubeTable, Set<TableRelationship>> getCubeGraph(CubeInterface cube) {
    return cubeToGraph.get(cube);
  }

  public Map<AbstractCubeTable, Set<TableRelationship>> getDimOnlyGraph() {
    return dimOnlySubGraph;
  }

  /**
   * Build the schema graph for all cubes and dimensions
   * 
   * @return
   * @throws org.apache.hadoop.hive.ql.metadata.HiveException
   */
  private void buildSchemaGraph() throws HiveException {
    cubeToGraph = new HashMap<CubeInterface, Map<AbstractCubeTable, Set<TableRelationship>>>();
    for (CubeInterface cube : metastore.getAllCubes()) {
      Map<AbstractCubeTable, Set<TableRelationship>> graph = new HashMap<AbstractCubeTable, Set<TableRelationship>>();
      buildGraph((AbstractCubeTable) cube, graph);

      for (Dimension dim : metastore.getAllDimensions()) {
        buildGraph(dim, graph);
      }

      cubeToGraph.put(cube, graph);
    }

    dimOnlySubGraph = new HashMap<AbstractCubeTable, Set<TableRelationship>>();
    for (Dimension dim : metastore.getAllDimensions()) {
      buildGraph(dim, dimOnlySubGraph);
    }
  }

  private List<CubeDimAttribute> getRefDimensions(AbstractCubeTable cube) throws HiveException {
    List<CubeDimAttribute> refDimensions = new ArrayList<CubeDimAttribute>();
    Set<CubeDimAttribute> allAttrs = null;
    if (cube instanceof CubeInterface) {
      allAttrs = ((CubeInterface) cube).getDimAttributes();
    } else if (cube instanceof Dimension) {
      allAttrs = ((Dimension) cube).getAttributes();
    } else {
      throw new HiveException("Not a valid table type" + cube);
    }
    // find out all dimensions which link to other dimension tables
    for (CubeDimAttribute dim : allAttrs) {
      if (dim instanceof ReferencedDimAtrribute) {
        if (((ReferencedDimAtrribute) dim).useAsJoinKey()) {
          refDimensions.add(dim);
        }
      } else if (dim instanceof HierarchicalDimAttribute) {
        for (CubeDimAttribute hdim : ((HierarchicalDimAttribute) dim).getHierarchy()) {
          if (hdim instanceof ReferencedDimAtrribute && ((ReferencedDimAtrribute) hdim).useAsJoinKey()) {
            refDimensions.add(hdim);
          }
        }
      }
    }
    return refDimensions;
  }

  // Build schema graph for a cube
  private void buildGraph(AbstractCubeTable cubeTable, Map<AbstractCubeTable, Set<TableRelationship>> graph)
      throws HiveException {
    List<CubeDimAttribute> refDimensions = getRefDimensions(cubeTable);

    // build graph for each linked dimension
    for (CubeDimAttribute dim : refDimensions) {
      // Find out references leading from dimension columns of the cube if any
      if (dim instanceof ReferencedDimAtrribute) {
        ReferencedDimAtrribute refDim = (ReferencedDimAtrribute) dim;
        List<TableReference> refs = refDim.getReferences();

        for (TableReference ref : refs) {
          String destColumnName = ref.getDestColumn();
          String destTableName = ref.getDestTable();

          if (metastore.isDimension(destTableName)) {
            // Cube -> Dimension reference
            Dimension relatedDim = metastore.getDimension(destTableName);
            addLinks(refDim.getName(), cubeTable, destColumnName, relatedDim, graph);
          } else {
            throw new HiveException("Dim -> Cube references are not supported: " + dim.getName() + "."
                + refDim.getName() + "->" + destTableName + "." + destColumnName);
          }
        } // end loop for refs from a dim
      }
    }
  }

  private void addLinks(String col1, AbstractCubeTable tab1, String col2, AbstractCubeTable tab2,
      Map<AbstractCubeTable, Set<TableRelationship>> graph) {

    TableRelationship rel1 = new TableRelationship(col1, tab1, col2, tab2);
    TableRelationship rel2 = new TableRelationship(col2, tab2, col1, tab1);

    Set<TableRelationship> inEdges = graph.get(tab2);
    if (inEdges == null) {
      inEdges = new LinkedHashSet<TableRelationship>();
      graph.put(tab2, inEdges);
    }
    inEdges.add(rel1);

    Set<TableRelationship> outEdges = graph.get(tab1);
    if (outEdges == null) {
      outEdges = new LinkedHashSet<TableRelationship>();
      graph.put(tab1, outEdges);
    }

    outEdges.add(rel2);

  }

  public void print() {
    for (CubeInterface cube : cubeToGraph.keySet()) {
      Map<AbstractCubeTable, Set<TableRelationship>> graph = cubeToGraph.get(cube);
      System.out.println("**Cube " + cube.getName());
      System.out.println("--Graph-Nodes=" + graph.size());
      for (AbstractCubeTable tab : graph.keySet()) {
        System.out.println(tab.getName() + "::" + graph.get(tab));
      }
    }
    System.out.println("**Dim only subgraph");
    System.out.println("--Graph-Nodes=" + dimOnlySubGraph.size());
    for (AbstractCubeTable tab : dimOnlySubGraph.keySet()) {
      System.out.println(tab.getName() + "::" + dimOnlySubGraph.get(tab));
    }
  }
}
