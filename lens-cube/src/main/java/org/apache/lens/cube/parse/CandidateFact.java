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

import static org.apache.hadoop.hive.ql.parse.HiveParser.DOT;
import static org.apache.hadoop.hive.ql.parse.HiveParser.Identifier;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_FUNCTION;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TABLE_OR_COL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.runtime.CommonToken;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.lens.cube.metadata.AbstractCubeTable;
import org.apache.lens.cube.metadata.CubeFactTable;
import org.apache.lens.cube.metadata.CubeInterface;
import org.apache.lens.cube.parse.HQLParser.ASTNodeVisitor;
import org.apache.lens.cube.parse.HQLParser.TreeNode;

/**
 * Holds context of a candidate fact table.
 * 
 */
class CandidateFact implements CandidateTable {
  public static Log LOG = LogFactory.getLog(CandidateFact.class.getName());
  final CubeFactTable fact;
  Set<String> storageTables;
  // flag to know if querying multiple storage tables is enabled for this fact
  boolean enabledMultiTableSelect;
  int numQueriedParts = 0;
  final Map<TimeRange, String> rangeToWhereClause = new HashMap<TimeRange, String>();
  private boolean dbResolved = false;
  private CubeInterface baseTable;
  private ASTNode selectAST;
  private ASTNode whereAST;
  private ASTNode groupbyAST;
  private ASTNode havingAST;
  List<TimeRangeNode> timenodes = new ArrayList<TimeRangeNode>();
  private final List<Integer> selectIndices = new ArrayList<Integer>();
  private final List<Integer> dimFieldIndices = new ArrayList<Integer>();
  private Collection<String> columns;

  CandidateFact(CubeFactTable fact, CubeInterface cube) {
    this.fact = fact;
    this.baseTable = cube;
  }

  @Override
  public String toString() {
    return fact.toString();
  }

  public Collection<String> getColumns() {
    if (columns == null) {
      columns = fact.getValidColumns();
      if (columns == null) {
        columns = fact.getAllFieldNames();
      }
    }
    return columns;
  }

  static class TimeRangeNode {
    ASTNode timenode;
    ASTNode parent;
    int childIndex;

    TimeRangeNode(ASTNode timenode, ASTNode parent, int childIndex) {
      this.timenode = timenode;
      this.parent = parent;
      this.childIndex = childIndex;
    }
  }

  private void updateTimeRanges(ASTNode root, ASTNode parent, int childIndex) throws SemanticException {
    if (root == null) {
      return;
    } else if (root.getToken().getType() == TOK_FUNCTION) {
      ASTNode fname = HQLParser.findNodeByPath(root, Identifier);
      if (fname != null && CubeQueryContext.TIME_RANGE_FUNC.equalsIgnoreCase(fname.getText())) {
        timenodes.add(new TimeRangeNode(root, parent, childIndex));
      }
    } else {
      for (int i = 0; i < root.getChildCount(); i++) {
        ASTNode child = (ASTNode) root.getChild(i);
        updateTimeRanges(child, root, i);
      }
    }
  }

  // copy ASTs from CubeQueryContext
  public void copyASTs(CubeQueryContext cubeql) throws SemanticException {
    this.selectAST = HQLParser.copyAST(cubeql.getSelectAST());
    this.whereAST = HQLParser.copyAST(cubeql.getWhereAST());
    if (cubeql.getGroupByAST() != null) {
      this.groupbyAST = HQLParser.copyAST(cubeql.getGroupByAST());
    }
    if (cubeql.getHavingAST() != null) {
      this.havingAST = HQLParser.copyAST(cubeql.getHavingAST());
    }
    // copy timeranges
    updateTimeRanges(this.whereAST, null, 0);
  }

  public void updateTimeranges(CubeQueryContext cubeql) throws SemanticException {
    // Update WhereAST with range clause
    // resolve timerange positions and replace it by corresponding where clause
    for (int i = 0; i < cubeql.getTimeRanges().size(); i++) {
      TimeRange range = cubeql.getTimeRanges().get(i);
      String rangeWhere = rangeToWhereClause.get(range);
      if (!StringUtils.isBlank(rangeWhere)) {
        ASTNode rangeAST;
        try {
          rangeAST = HQLParser.parseExpr(rangeWhere);
        } catch (ParseException e) {
          throw new SemanticException(e);
        }
        rangeAST.setParent(timenodes.get(i).parent);
        timenodes.get(i).parent.setChild(timenodes.get(i).childIndex, rangeAST);
      }
    }
  }

  /**
   * Update the ASTs to include only the fields queried from this fact, in all
   * the expressions
   * 
   * @param cubeql
   * @throws SemanticException
   */
  public void updateASTs(CubeQueryContext cubeql) throws SemanticException {
    Set<String> cubeColsQueried = cubeql.getColumnsQueried(cubeql.getCube().getName());

    // update select AST with selected fields
    int currentChild = 0;
    for (int i = 0; i < cubeql.getSelectAST().getChildCount(); i++) {
      ASTNode selectExpr = (ASTNode) this.selectAST.getChild(currentChild);
      Set<String> exprCols = getColsInExpr(cubeColsQueried, selectExpr);
      if (getColumns().containsAll(exprCols)) {
        selectIndices.add(i);
        if (cubeql.getQueriedDimAttrs().containsAll(exprCols)) {
          dimFieldIndices.add(i);
        }
        ASTNode aliasNode = HQLParser.findNodeByPath(selectExpr, Identifier);
        String alias = cubeql.getSelectAlias(i);
        if (aliasNode != null) {
          String queryAlias = aliasNode.getText();
          if (!queryAlias.equals(alias)) {
            // replace the alias node
            ASTNode newAliasNode = new ASTNode(new CommonToken(HiveParser.Identifier, alias));
            this.selectAST.getChild(currentChild).replaceChildren(selectExpr.getChildCount() - 1,
                selectExpr.getChildCount() - 1, newAliasNode);
          }
        } else {
          // add column alias
          ASTNode newAliasNode = new ASTNode(new CommonToken(HiveParser.Identifier, alias));
          this.selectAST.getChild(currentChild).addChild(newAliasNode);
        }
      } else {
        this.selectAST.deleteChild(currentChild);
        currentChild--;
      }
      currentChild++;
    }

    // update whereAST to include only filters of this fact
    // TODO

    // update havingAST to include only filters of this fact
    // TODO
  }

  private Set<String> getColsInExpr(final Set<String> cubeCols, ASTNode expr) throws SemanticException {
    final Set<String> cubeColsInExpr = new HashSet<String>();
    HQLParser.bft(expr, new ASTNodeVisitor() {
      @Override
      public void visit(TreeNode visited) {
        ASTNode node = visited.getNode();
        ASTNode parent = null;
        if (visited.getParent() != null) {
          parent = visited.getParent().getNode();
        }

        if (node.getToken().getType() == TOK_TABLE_OR_COL && (parent != null && parent.getToken().getType() != DOT)) {
          // Take child ident.totext
          ASTNode ident = (ASTNode) node.getChild(0);
          String column = ident.getText().toLowerCase();
          if (cubeCols.contains(column)) {
            cubeColsInExpr.add(column);
          }
        } else if (node.getToken().getType() == DOT) {
          ASTNode colIdent = (ASTNode) node.getChild(1);
          String column = colIdent.getText().toLowerCase();
          if (cubeCols.contains(column)) {
            cubeColsInExpr.add(column);
          }
        }
      }
    });

    return cubeColsInExpr;
  }

  public String getStorageString(String alias) {
    if (!dbResolved) {
      String database = SessionState.get().getCurrentDatabase();
      // Add database name prefix for non default database
      if (StringUtils.isNotBlank(database) && !"default".equalsIgnoreCase(database)) {
        Set<String> storageTbls = new HashSet<String>();
        Iterator<String> names = storageTables.iterator();
        while (names.hasNext()) {
          storageTbls.add(database + "." + names.next());
        }
        this.storageTables = storageTbls;
      }
      dbResolved = true;
    }
    return StringUtils.join(storageTables, ",") + " " + alias;
  }

  @Override
  public AbstractCubeTable getBaseTable() {
    return (AbstractCubeTable) baseTable;
  }

  @Override
  public CubeFactTable getTable() {
    return fact;
  }

  @Override
  public String getName() {
    return fact.getName();
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    CandidateFact other = (CandidateFact) obj;

    if (this.getTable() == null) {
      if (other.getTable() != null) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((getTable() == null) ? 0 : getTable().getName().toLowerCase().hashCode());
    return result;
  }

  public String getSelectTree() {
    return HQLParser.getString(selectAST);
  }

  public String getWhereTree() {
    if (whereAST != null) {
      return HQLParser.getString(whereAST);
    }
    return null;
  }

  public String getHavingTree() {
    if (havingAST != null) {
      return HQLParser.getString(havingAST);
    }
    return null;
  }

  public String getGroupbyTree() {
    if (groupbyAST != null) {
      return HQLParser.getString(groupbyAST);
    }
    return null;
  }

  /**
   * @return the selectAST
   */
  public ASTNode getSelectAST() {
    return selectAST;
  }

  /**
   * @param selectAST
   *          the selectAST to set
   */
  public void setSelectAST(ASTNode selectAST) {
    this.selectAST = selectAST;
  }

  /**
   * @return the whereAST
   */
  public ASTNode getWhereAST() {
    return whereAST;
  }

  /**
   * @param whereAST
   *          the whereAST to set
   */
  public void setWhereAST(ASTNode whereAST) {
    this.whereAST = whereAST;
  }

  /**
   * @return the havingAST
   */
  public ASTNode getHavingAST() {
    return havingAST;
  }

  /**
   * @param havingAST
   *          the havingAST to set
   */
  public void setHavingAST(ASTNode havingAST) {
    this.havingAST = havingAST;
  }

  /**
   * @return the selectIndices
   */
  public List<Integer> getSelectIndices() {
    return selectIndices;
  }

  /**
   * @return the groupbyIndices
   */
  public List<Integer> getDimFieldIndices() {
    return dimFieldIndices;
  }

  public ASTNode getGroupByAST() {
    return groupbyAST;
  }

  public String getGroupByTree() {
    if (groupbyAST != null) {
      return HQLParser.getString(groupbyAST);
    }
    return null;
  }
}
