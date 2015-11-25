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
package org.apache.lens.cube.parse;

import java.util.*;

import org.apache.lens.cube.metadata.AbstractCubeTable;
import org.apache.lens.cube.metadata.Dimension;
import org.apache.lens.cube.metadata.SchemaGraph;

import org.apache.hadoop.hive.ql.parse.JoinType;

import lombok.Getter;
import lombok.ToString;

@ToString
public class JoinClause implements Comparable<JoinClause> {
  private final int cost;
  // all dimensions in path except target
  @Getter
  private final Set<Dimension> dimsInPath;
  private CubeQueryContext cubeql;
  private final Map<Aliased<Dimension>, List<SchemaGraph.TableRelationship>> chain;
  @Getter
  private final JoinTree joinTree;
  transient Map<AbstractCubeTable, Set<String>> chainColumns = new HashMap<AbstractCubeTable, Set<String>>();

  public JoinClause(CubeQueryContext cubeql, Map<Aliased<Dimension>,
    List<SchemaGraph.TableRelationship>> chain, Set<Dimension> dimsInPath) {
    this.cubeql = cubeql;
    this.chain = chain;
    this.joinTree = mergeJoinChains(chain);
    this.cost = joinTree.getNumEdges();
    this.dimsInPath = dimsInPath;
  }

  void initChainColumns() {
    for (List<SchemaGraph.TableRelationship> path : chain.values()) {
      for (SchemaGraph.TableRelationship edge : path) {
        Set<String> fcols = chainColumns.get(edge.getFromTable());
        if (fcols == null) {
          fcols = new HashSet<String>();
          chainColumns.put(edge.getFromTable(), fcols);
        }
        fcols.add(edge.getFromColumn());

        Set<String> tocols = chainColumns.get(edge.getToTable());
        if (tocols == null) {
          tocols = new HashSet<String>();
          chainColumns.put(edge.getToTable(), tocols);
        }
        tocols.add(edge.getToColumn());
      }
    }
  }

  public int getCost() {
    return cost;
  }

  @Override
  public int compareTo(JoinClause joinClause) {
    return cost - joinClause.getCost();
  }

  /**
   * Takes chains and merges them in the form of a tree. If two chains have some common path till some table and
   * bifurcate from there, then in the chain, both paths will have the common path but the resultant tree will have
   * single path from root(cube) to that table and paths will bifurcate from there.
   * <p/>
   * For example, citystate   =   [basecube.cityid=citydim.id], [citydim.stateid=statedim.id]
   *              cityzip     =   [basecube.cityid=citydim.id], [citydim.zipcode=zipdim.code]
   * <p/>
   * Without merging, the behaviour is like this:
   * <p/>
   * <p/>
   *                  (basecube.cityid=citydim.id)          (citydim.stateid=statedim.id)
   *                  _____________________________citydim____________________________________statedim
   *                 |
   *   basecube------|
   *                 |_____________________________citydim____________________________________zipdim
   *
   *                  (basecube.cityid=citydim.id)          (citydim.zipcode=zipdim.code)
   *
   * <p/>
   * Merging will result in a tree like following
   * <p/>                                                  (citydim.stateid=statedim.id)
   * <p/>                                                ________________________________ statedim
   *             (basecube.cityid=citydim.id)           |
   * basecube-------------------------------citydim---- |
   *                                                    |________________________________  zipdim
   *
   *                                                       (citydim.zipcode=zipdim.code)
   *
   * <p/>
   * Doing this will reduce the number of joins wherever possible.
   *
   * @param chain Joins in Linear format.
   * @return Joins in Tree format
   */
  public JoinTree mergeJoinChains(Map<Aliased<Dimension>, List<SchemaGraph.TableRelationship>> chain) {
    Map<String, Integer> aliasUsage = new HashMap<String, Integer>();
    JoinTree root = JoinTree.createRoot();
    for (Map.Entry<Aliased<Dimension>, List<SchemaGraph.TableRelationship>> entry : chain.entrySet()) {
      JoinTree current = root;
      // Last element in this list is link from cube to first dimension
      for (int i = entry.getValue().size() - 1; i >= 0; i--) {
        // Adds a child if needed, or returns a child already existing corresponding to the given link.
        current = current.addChild(entry.getValue().get(i), cubeql, aliasUsage);
        if (cubeql.getAutoJoinCtx().isPartialJoinChains()) {
          JoinType joinType = cubeql.getAutoJoinCtx().getTableJoinTypeMap().get(entry.getKey().getObject());
          //This ensures if (sub)paths are same, but join type is not same, merging will not happen.
          current.setJoinType(joinType);
        }
      }
      // This is a destination table. Decide alias separately. e.g. chainname
      // nullcheck is necessary because dimensions can be destinations too. In that case getAlias() == null
      if (entry.getKey().getAlias() != null) {
        current.setAlias(entry.getKey().getAlias());
      }
    }
    if (root.getSubtrees().size() > 0) {
      root.setAlias(cubeql.getAliasForTableName(
        root.getSubtrees().keySet().iterator().next().getFromTable().getName()));
    }
    return root;
  }
}
