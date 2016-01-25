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
package org.apache.lens.cube.parse.join;

import java.util.*;

import org.apache.lens.cube.metadata.AbstractCubeTable;
import org.apache.lens.cube.metadata.join.TableRelationship;

import org.apache.hadoop.hive.ql.parse.JoinType;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString(exclude = "parent")
@EqualsAndHashCode(exclude = "parent")
public class JoinTree {
  //parent of the node
  JoinTree parent;
  // current table is parentRelationship.destTable;
  TableRelationship parentRelationship;
  // Alias for the join clause
  String alias;
  private Map<TableRelationship, JoinTree> subtrees = new LinkedHashMap<>();
  // Number of nodes from root to this node. depth of root is 0. Unused for now.
  private int depthFromRoot;
  // join type of the current table.
  JoinType joinType;

  public static JoinTree createRoot() {
    return new JoinTree(null, null, 0);
  }

  public JoinTree(JoinTree parent, TableRelationship tableRelationship,
                  int depthFromRoot) {
    this.parent = parent;
    this.parentRelationship = tableRelationship;
    this.depthFromRoot = depthFromRoot;
  }

  public JoinTree addChild(TableRelationship tableRelationship, Map<String, Integer> aliasUsage) {
    if (getSubtrees().get(tableRelationship) == null) {
      JoinTree current = new JoinTree(this, tableRelationship,
        this.depthFromRoot + 1);
      // Set alias. Need to compute only when new node is being created.
      // The following code ensures that For intermediate tables, aliases are given
      // in the order cityDim, cityDim_0, cityDim_1, ...
      // And for destination tables, an alias will be decided from here but might be
      // overridden outside this function.
      AbstractCubeTable destTable = tableRelationship.getToTable();
      current.setAlias(destTable.getName());
      if (aliasUsage.get(current.getAlias()) == null) {
        aliasUsage.put(current.getAlias(), 0);
      } else {
        aliasUsage.put(current.getAlias(), aliasUsage.get(current.getAlias()) + 1);
        current.setAlias(current.getAlias() + "_" + (aliasUsage.get(current.getAlias()) - 1));
      }
      getSubtrees().put(tableRelationship, current);
    }
    return getSubtrees().get(tableRelationship);
  }

  // Recursive computation of number of edges.
  public int getNumEdges() {
    int ret = 0;
    for (JoinTree tree : getSubtrees().values()) {
      ret += 1;
      ret += tree.getNumEdges();
    }
    return ret;
  }

  public boolean isLeaf() {
    return getSubtrees().isEmpty();
  }

  // Breadth First Traversal. Unused currently.
  public Iterator<JoinTree> bft() {
    return new Iterator<JoinTree>() {
      List<JoinTree> remaining = new ArrayList<JoinTree>() {
        {
          addAll(getSubtrees().values());
        }
      };

      @Override
      public boolean hasNext() {
        return remaining.isEmpty();
      }

      @Override
      public JoinTree next() {
        JoinTree retVal = remaining.remove(0);
        remaining.addAll(retVal.getSubtrees().values());
        return retVal;
      }

      @Override
      public void remove() {
        throw new RuntimeException("Not implemented");
      }
    };
  }

  // Depth first traversal of the tree. Used in forming join string.
  public Iterator<JoinTree> dft() {
    return new Iterator<JoinTree>() {
      Stack<JoinTree> joinTreeStack = new Stack<JoinTree>() {
        {
          addAll(getSubtrees().values());
        }
      };

      @Override
      public boolean hasNext() {
        return !joinTreeStack.isEmpty();
      }

      @Override
      public JoinTree next() {
        JoinTree retVal = joinTreeStack.pop();
        joinTreeStack.addAll(retVal.getSubtrees().values());
        return retVal;
      }

      @Override
      public void remove() {
        throw new RuntimeException("Not implemented");
      }
    };
  }

  public Set<JoinTree> leaves() {
    Set<JoinTree> leaves = new HashSet<>();
    Iterator<JoinTree> dft = dft();
    while (dft.hasNext()) {
      JoinTree cur = dft.next();
      if (cur.isLeaf()) {
        leaves.add(cur);
      }
    }
    return leaves;
  }
}
