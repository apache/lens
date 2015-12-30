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

import org.apache.lens.cube.metadata.SchemaGraph.JoinPath;
import org.apache.lens.cube.metadata.SchemaGraph.TableRelationship;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class JoinChain implements Named {
  @Getter
  private final String name;
  @Getter
  private final String displayString;
  @Getter
  private final String description;
  // There can be more than one path associated with same name.
  // c1.r1->t1.k1
  // c1.r2->t1.k2
  @Getter
  private final List<Path> paths;


  public void addProperties(AbstractCubeTable tbl) {
    if (tbl instanceof Cube) {
      addProperties((Cube) tbl);
    } else {
      addProperties((Dimension) tbl);
    }
  }

  public void addProperties(Cube cube) {
    Map<String, String> props = cube.getProperties();
    props.put(MetastoreUtil.getCubeJoinChainNumChainsKey(getName()), String.valueOf(paths.size()));
    for (int i = 0; i < paths.size(); i++) {
      props.put(MetastoreUtil.getCubeJoinChainFullChainKey(getName(), i),
        MetastoreUtil.getReferencesString(paths.get(i).getReferences()));
    }
    if (displayString != null) {
      props.put(MetastoreUtil.getCubeJoinChainDisplayKey(getName()), displayString);
    }
    if (description != null) {
      props.put(MetastoreUtil.getCubeJoinChainDescriptionKey(getName()), description);
    }
  }

  public void addProperties(Dimension dimension) {
    Map<String, String> props = dimension.getProperties();
    props.put(MetastoreUtil.getDimensionJoinChainNumChainsKey(getName()), String.valueOf(paths.size()));
    for (int i = 0; i < paths.size(); i++) {
      props.put(MetastoreUtil.getDimensionJoinChainFullChainKey(getName(), i),
        MetastoreUtil.getReferencesString(paths.get(i).getReferences()));
    }
    if (displayString != null) {
      props.put(MetastoreUtil.getDimensionJoinChainDisplayKey(getName()), displayString);
    }
    if (description != null) {
      props.put(MetastoreUtil.getDimensionJoinChainDescriptionKey(getName()), description);
    }
  }


  /**
   * Construct join chain
   *
   * @param name
   * @param display
   * @param description
   */
  public JoinChain(String name, String display, String description) {
    this.name = name.toLowerCase();
    this.displayString = display;
    this.description = description;
    this.paths = new ArrayList<Path>();
  }

  /**
   * This is used only for serializing
   *
   * @param table
   * @param name
   */
  public JoinChain(AbstractBaseTable table, String name) {
    boolean isCube = (table instanceof Cube);
    this.name = name;
    this.paths = new ArrayList<Path>();
    int numChains = 0;

    Map<String, String> props = table.getProperties();
    if (isCube) {
      numChains = Integer.parseInt(props.get(MetastoreUtil.getCubeJoinChainNumChainsKey(getName())));
    } else {
      numChains = Integer.parseInt(props.get(MetastoreUtil.getDimensionJoinChainNumChainsKey(getName())));
    }
    for (int i = 0; i < numChains; i++) {
      Path chain = new Path();
      String refListStr;
      if (isCube) {
        refListStr = props.get(MetastoreUtil.getCubeJoinChainFullChainKey(getName(), i));
      } else {
        refListStr = props.get(MetastoreUtil.getDimensionJoinChainFullChainKey(getName(), i));
      }
      String[] refListDims = StringUtils.split(refListStr, ",");
      TableReference from = null;
      for (String refDimRaw : refListDims) {
        if (from == null) {
          from = new TableReference(refDimRaw);
        } else {
          chain.addLink(new Edge(from, new TableReference(refDimRaw)));
          from = null;
        }
      }
      paths.add(chain);
    }
    if (isCube) {
      this.description = props.get(MetastoreUtil.getCubeJoinChainDescriptionKey(name));
      this.displayString = props.get(MetastoreUtil.getCubeJoinChainDisplayKey(name));
    } else {
      this.description = props.get(MetastoreUtil.getDimensionJoinChainDescriptionKey(name));
      this.displayString = props.get(MetastoreUtil.getDimensionJoinChainDisplayKey(name));
    }

  }

  /**
   * Copy constructor for JoinChain
   *
   * @param other JoinChain
   */
  public JoinChain(JoinChain other) {
    this.name = other.name;
    this.displayString = other.displayString;
    this.description = other.description;
    this.paths = new ArrayList<Path>(other.paths);
  }

  @EqualsAndHashCode(exclude = {"relationShip"})
  @ToString
  public static class Edge {
    @Getter
    private final TableReference from;
    @Getter
    private final TableReference to;
    transient TableRelationship relationShip = null;

    Edge(TableReference from, TableReference to) {
      this.from = from;
      this.to = to;
    }

    TableRelationship toDimToDimRelationship(CubeMetastoreClient client) throws HiveException {
      if (relationShip == null) {
        relationShip = new TableRelationship(from.getDestColumn(),
          client.getDimension(from.getDestTable()),
          to.getDestColumn(),
          client.getDimension(to.getDestTable()),
          to.isMapsToMany());
      }
      return relationShip;
    }

    /**
     * return Cube or Dimension relationship depending on the source table of the join chain.
     *
     * @param client
     * @return
     * @throws HiveException
     */
    TableRelationship toCubeOrDimRelationship(CubeMetastoreClient client) throws HiveException {
      if (relationShip == null) {
        AbstractCubeTable fromTable = null;
        if (client.isCube(from.getDestTable())) {
          fromTable = (AbstractCubeTable) client.getCube(from.getDestTable());
        } else if (client.isDimension(from.getDestTable())) {
          fromTable = client.getDimension(from.getDestTable());
        }

        if (fromTable != null) {
          relationShip = new TableRelationship(from.getDestColumn(),
            fromTable,
            to.getDestColumn(),
            client.getDimension(to.getDestTable()),
            to.isMapsToMany());
        }
      }
      return relationShip;
    }

  }

  @EqualsAndHashCode(exclude = {"refs"})
  @ToString
  public static class Path {
    @Getter
    final List<Edge> links = new ArrayList<Edge>();
    transient List<TableReference> refs = null;

    private void addLink(Edge edge) {
      links.add(edge);
    }

    public List<TableReference> getReferences() {
      if (refs == null) {
        refs = new ArrayList<TableReference>();
        for (Edge edge : links) {
          refs.add(edge.from);
          refs.add(edge.to);
        }
      }
      return refs;
    }

    Path() {
    }

    Path(List<TableReference> refs) {
      for (int i = 0; i < refs.size() - 1; i += 2) {
        addLink(new Edge(refs.get(i), refs.get(i + 1)));
      }
    }

    String getDestTable() {
      return links.get(links.size() - 1).to.getDestTable();
    }

    String getSrcColumn() {
      return links.get(0).from.getDestColumn();
    }
  }

  public void addPath(List<TableReference> refs) {
    if (refs.size() <= 1 || refs.size() % 2 != 0) {
      throw new IllegalArgumentException("Path should contain both from and to links");
    }
    this.paths.add(new Path(refs));
  }

  private transient String destTable = null;

  /**
   * Get final destination table
   *
   * @return
   */
  public String getDestTable() {
    if (destTable == null) {
      for (Path path : paths) {
        if (destTable == null) {
          destTable = path.getDestTable();
        } else {
          String temp = path.getDestTable();
          if (!destTable.equalsIgnoreCase(temp)) {
            throw new IllegalArgumentException("Paths have different destination tables :" + destTable + "," + temp);
          }
        }
      }
    }
    return destTable;
  }

  /**
   * Get all source columns from cube
   *
   * @return
   */
  public Set<String> getSourceColumns() {
    Set<String> srcFields = new HashSet<String>();
    for (Path path : paths) {
      srcFields.add(path.getSrcColumn());
    }
    return srcFields;
  }

  /**
   * Get all dimensions involved in paths, except destination table
   *
   * @return
   */
  public Set<String> getIntermediateDimensions() {
    Set<String> dimNames = new HashSet<String>();
    for (Path path : paths) {
      // skip last entry in all paths, as that would be a destination
      for (int i = 0; i < path.links.size() - 1; i++) {
        dimNames.add(path.links.get(i).to.getDestTable());
      }
    }
    return dimNames;
  }

  /**
   * Convert join paths to schemaGraph's JoinPath
   *
   * @param client
   * @return List&lt;SchemaGraph.JoinPath&gt;
   * @throws HiveException
   */
  public List<SchemaGraph.JoinPath> getRelationEdges(CubeMetastoreClient client) throws HiveException {
    List<SchemaGraph.JoinPath> schemaGraphPaths = new ArrayList<SchemaGraph.JoinPath>();
    for (Path path : paths) {
      JoinPath jp = new JoinPath();
      // Add edges from dimension to cube
      for (int i = path.links.size() - 1; i > 0; i -= 1) {
        jp.addEdge(path.links.get(i).toDimToDimRelationship(client));
      }
      jp.addEdge(path.links.get(0).toCubeOrDimRelationship(client));
      schemaGraphPaths.add(jp);
    }
    return schemaGraphPaths;
  }
}
