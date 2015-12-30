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

import org.apache.hadoop.hive.ql.metadata.HiveException;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

public class SchemaGraph {
  /*
   * An edge in the schema graph
   */
  @Data
  @AllArgsConstructor
  @RequiredArgsConstructor
  public static class TableRelationship {
    final String fromColumn;
    final AbstractCubeTable fromTable;
    final String toColumn;
    final AbstractCubeTable toTable;
    boolean mapsToMany = false;

    @Override
    public String toString() {
      return fromTable.getName() + "." + fromColumn + "->" + toTable.getName() + "." + toColumn
        + (mapsToMany ? "[n]" : "");
    }

  }

  /**
   * A list of table relationships that can be combined to get a join clause
   */
  public static class JoinPath {
    final List<TableRelationship> edges;
    // Store the map of a table against all columns of that table which are in the path
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
    // edges going out of the table
    private final Map<AbstractCubeTable, Set<TableRelationship>> outGraph;
    // egds coming into the table
    private final Map<AbstractCubeTable, Set<TableRelationship>> inGraph;
    // Used in tests to validate that all paths are searched

    public GraphSearch(AbstractCubeTable source, AbstractCubeTable target, SchemaGraph graph) {
      this.source = source;
      this.target = target;

      if (target instanceof CubeInterface) {
        this.outGraph = graph.getCubeGraph((CubeInterface) target);
        this.inGraph = graph.getCubeInGraph((CubeInterface) target);
      } else if (target instanceof Dimension) {
        this.outGraph = graph.getDimOnlyGraph();
        this.inGraph = graph.getDimOnlyInGraph();
      } else {
        throw new IllegalArgumentException("Target neither cube nor dimension");
      }
    }

    public List<JoinPath> findAllPathsToTarget() {
      return findAllPathsToTarget(source, new JoinPath(), new HashSet<AbstractCubeTable>());
    }

    /**
     * Recursive DFS to get all paths between source and target. Let path till this node = p Paths at node adjacent to
     * target = [edges leading to target] Path at a random node = [path till this node + p for each p in
     * path(neighbors)]
     */
    List<JoinPath> findAllPathsToTarget(AbstractCubeTable source, JoinPath joinPathTillSource,
      Set<AbstractCubeTable> visited) {
      List<JoinPath> joinPaths = new ArrayList<JoinPath>();
      visited.add(source);

      if (inGraph.get(source) == null) {
        return joinPaths;
      }
      for (TableRelationship edge : inGraph.get(source)) {
        if (visited.contains(edge.getFromTable())) {
          continue;
        }


        JoinPath p = new JoinPath(joinPathTillSource);
        p.addEdge(edge);
        AbstractCubeTable neighbor = edge.getFromTable();
        if (neighbor.getName().equals(target.getName())) {
          // Got a direct path
          joinPaths.add(p);
        } else if (neighbor instanceof Dimension) {
          List<JoinPath> pathsFromNeighbor = findAllPathsToTarget(neighbor, new JoinPath(p), visited);
          for (JoinPath pn : pathsFromNeighbor) {
            if (!pn.isEmpty()) {
              joinPaths.add(pn);
            }
          }
        }
      }

      return joinPaths;
    }
  }

  /**
   * Graph of tables in the cube metastore. Links between the tables are relationships in the cube.
   */
  private final CubeMetastoreClient metastore;
  // Graph for each cube
  // graph with out going edges
  private Map<CubeInterface, Map<AbstractCubeTable, Set<TableRelationship>>> cubeOutGraph;
  // graph with incoming edges
  private Map<CubeInterface, Map<AbstractCubeTable, Set<TableRelationship>>> cubeInGraph;

  // sub graph that contains only dimensions, mainly used while checking connectivity between a set of dimensions
  // graph with out going edges
  private Map<AbstractCubeTable, Set<TableRelationship>> dimOnlyOutGraph;
  // graph with incoming edges
  private Map<AbstractCubeTable, Set<TableRelationship>> dimOnlyInGraph;

  public SchemaGraph(CubeMetastoreClient metastore) throws HiveException {
    this.metastore = metastore;
    buildSchemaGraph();
  }

  public Map<AbstractCubeTable, Set<TableRelationship>> getCubeGraph(CubeInterface cube) {
    return cubeOutGraph.get(cube);
  }

  public Map<AbstractCubeTable, Set<TableRelationship>> getDimOnlyGraph() {
    return dimOnlyOutGraph;
  }

  public Map<AbstractCubeTable, Set<TableRelationship>> getCubeInGraph(CubeInterface cube) {
    return cubeInGraph.get(cube);
  }

  public Map<AbstractCubeTable, Set<TableRelationship>> getDimOnlyInGraph() {
    return dimOnlyInGraph;
  }

  /**
   * Build the schema graph for all cubes and dimensions
   *
   * @return
   * @throws org.apache.hadoop.hive.ql.metadata.HiveException
   */
  private void buildSchemaGraph() throws HiveException {
    cubeOutGraph = new HashMap<CubeInterface, Map<AbstractCubeTable, Set<TableRelationship>>>();
    cubeInGraph = new HashMap<CubeInterface, Map<AbstractCubeTable, Set<TableRelationship>>>();
    for (CubeInterface cube : metastore.getAllCubes()) {
      Map<AbstractCubeTable, Set<TableRelationship>> outGraph
        = new HashMap<AbstractCubeTable, Set<TableRelationship>>();
      Map<AbstractCubeTable, Set<TableRelationship>> inGraph
        = new HashMap<AbstractCubeTable, Set<TableRelationship>>();
      buildGraph((AbstractCubeTable) cube, outGraph, inGraph);

      for (Dimension dim : metastore.getAllDimensions()) {
        buildGraph(dim, outGraph, inGraph);
      }

      cubeOutGraph.put(cube, outGraph);
      cubeInGraph.put(cube, inGraph);
    }

    dimOnlyOutGraph = new HashMap<AbstractCubeTable, Set<TableRelationship>>();
    dimOnlyInGraph = new HashMap<AbstractCubeTable, Set<TableRelationship>>();
    for (Dimension dim : metastore.getAllDimensions()) {
      buildGraph(dim, dimOnlyOutGraph, dimOnlyInGraph);
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

  // Build schema graph for a cube/dimension
  private void buildGraph(AbstractCubeTable cubeTable, Map<AbstractCubeTable, Set<TableRelationship>> outGraph,
    Map<AbstractCubeTable, Set<TableRelationship>> inGraph)
    throws HiveException {
    List<CubeDimAttribute> refDimensions = getRefDimensions(cubeTable);

    // build graph for each linked dimension
    for (CubeDimAttribute dim : refDimensions) {
      // Find out references leading from dimension columns of the cube/dimension if any
      if (dim instanceof ReferencedDimAtrribute) {
        ReferencedDimAtrribute refDim = (ReferencedDimAtrribute) dim;
        List<TableReference> refs = refDim.getReferences();

        for (TableReference ref : refs) {
          String destColumnName = ref.getDestColumn();
          String destTableName = ref.getDestTable();

          if (metastore.isDimension(destTableName)) {
            // Cube -> Dimension or Dimension -> Dimension reference
            Dimension relatedDim = metastore.getDimension(destTableName);
            addLinks(refDim.getName(), cubeTable, destColumnName, relatedDim, ref.isMapsToMany(), outGraph, inGraph);
          } else {
            throw new HiveException("Dim -> Cube references are not supported: " + dim.getName() + "."
              + refDim.getName() + "->" + destTableName + "." + destColumnName);
          }
        } // end loop for refs from a dim
      }
    }
  }

  private void addLinks(String srcCol, AbstractCubeTable srcTbl, String destCol, AbstractCubeTable destTbl,
    boolean mapsToMany, Map<AbstractCubeTable, Set<TableRelationship>> outGraph,
    Map<AbstractCubeTable, Set<TableRelationship>> inGraph) {

    TableRelationship rel = new TableRelationship(srcCol, srcTbl, destCol, destTbl, mapsToMany);

    Set<TableRelationship> inEdges = inGraph.get(destTbl);
    if (inEdges == null) {
      inEdges = new LinkedHashSet<TableRelationship>();
      inGraph.put(destTbl, inEdges);
    }
    inEdges.add(rel);

    Set<TableRelationship> outEdges = outGraph.get(srcTbl);
    if (outEdges == null) {
      outEdges = new LinkedHashSet<TableRelationship>();
      outGraph.put(srcTbl, outEdges);
    }

    outEdges.add(rel);

  }

  public void print() {
    for (CubeInterface cube : cubeOutGraph.keySet()) {
      Map<AbstractCubeTable, Set<TableRelationship>> graph = cubeOutGraph.get(cube);
      System.out.println("**Cube " + cube.getName() + " Out egdes");
      System.out.println("--Out Graph-Nodes=" + graph.size());
      for (AbstractCubeTable tab : graph.keySet()) {
        System.out.println(tab.getName() + "::" + graph.get(tab));
      }
    }
    System.out.println("**Dim only outgraph");
    System.out.println("--Out Graph-Nodes=" + dimOnlyOutGraph.size());
    for (AbstractCubeTable tab : dimOnlyOutGraph.keySet()) {
      System.out.println(tab.getName() + "::" + dimOnlyOutGraph.get(tab));
    }

    for (CubeInterface cube : cubeInGraph.keySet()) {
      Map<AbstractCubeTable, Set<TableRelationship>> graph = cubeInGraph.get(cube);
      System.out.println("**Cube " + cube.getName() + " In egdes");
      System.out.println("--In Graph-Nodes=" + graph.size());
      for (AbstractCubeTable tab : graph.keySet()) {
        System.out.println(tab.getName() + "::" + graph.get(tab));
      }
    }
    System.out.println("**Dim only Ingraph");
    System.out.println("--In Graph-Nodes=" + dimOnlyInGraph.size());
    for (AbstractCubeTable tab : dimOnlyInGraph.keySet()) {
      System.out.println(tab.getName() + "::" + dimOnlyInGraph.get(tab));
    }

  }
}
