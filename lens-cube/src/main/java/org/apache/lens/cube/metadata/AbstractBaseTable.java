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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.base.Preconditions;
import lombok.Getter;

/**
 * Abstract table with expressions
 */


public abstract class AbstractBaseTable extends AbstractCubeTable {
  private final Set<ExprColumn> expressions;
  private static final List<FieldSchema> COLUMNS = new ArrayList<FieldSchema>();
  private final Map<String, ExprColumn> exprMap;
  @Getter
  private final Set<JoinChain> joinChains;
  private final Map<String, JoinChain> chainMap;

  static {
    COLUMNS.add(new FieldSchema("dummy", "string", "dummy column"));
  }

  public AbstractBaseTable(String name, Set<ExprColumn> exprs, Set<JoinChain> joinChains, Map<String, String>
    properties, double weight) {
    super(name, COLUMNS, properties, weight);

    exprMap = new HashMap<String, ExprColumn>();
    if (exprs == null) {
      this.expressions = new HashSet<ExprColumn>();
    } else {
      this.expressions = exprs;
    }

    for (ExprColumn expr : expressions) {
      exprMap.put(expr.getName().toLowerCase(), expr);
    }

    if (joinChains != null) {
      this.joinChains = joinChains;
    } else {
      this.joinChains = new HashSet<JoinChain>();
    }

    chainMap = new HashMap<String, JoinChain>();
    for (JoinChain chain : this.joinChains) {
      chainMap.put(chain.getName().toLowerCase(), chain);
    }
  }

  public AbstractBaseTable(Table tbl) {
    super(tbl);
    this.expressions = getExpressions(getName(), getProperties());
    exprMap = new HashMap<String, ExprColumn>();
    for (ExprColumn expr : expressions) {
      exprMap.put(expr.getName().toLowerCase(), expr);
    }
    this.joinChains = getJoinChains();
    chainMap = new HashMap<String, JoinChain>();
    for (JoinChain chain : joinChains) {
      chainMap.put(chain.getName().toLowerCase(), chain);
    }
  }

  @Override
  public Set<String> getStorages() {
    return null;
  }

  @Override
  public void addProperties() {
    super.addProperties();
    MetastoreUtil.addNameStrings(getProperties(), MetastoreUtil.getExpressionListKey(getName()), expressions);
    setExpressionProperties(getProperties(), expressions);
    MetastoreUtil.addNameStrings(getProperties(), getJoinChainListPropKey(getName()), joinChains);
    setJoinChainProperties(joinChains);
  }

  private static void setExpressionProperties(Map<String, String> props, Set<ExprColumn> expressions) {
    for (ExprColumn expr : expressions) {
      expr.addProperties(props);
    }
  }

  private static Set<ExprColumn> getExpressions(String name, Map<String, String> props) {
    Set<ExprColumn> exprs = new HashSet<ExprColumn>();
    String exprStr = MetastoreUtil.getNamedStringValue(props, MetastoreUtil.getExpressionListKey(name));
    if (!StringUtils.isBlank(exprStr)) {
      String[] names = exprStr.split(",");
      for (String exprName : names) {
        ExprColumn expr = new ExprColumn(exprName, props);
        exprs.add(expr);
      }
    }
    return exprs;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    AbstractBaseTable other = (AbstractBaseTable) obj;
    if (this.getExpressions() == null) {
      if (other.getExpressions() != null) {
        return false;
      }
    } else if (!this.getExpressions().equals(other.getExpressions())) {
      return false;
    }

    if (this.getJoinChains() == null) {
      if (other.getJoinChains() != null) {
        return false;
      }
    } else if (!this.getJoinChains().equals(other.getJoinChains())) {
      return false;
    }
    return true;
  }

  public ExprColumn getExpressionByName(String exprName) {
    return exprMap.get(exprName == null ? exprName : exprName.toLowerCase());
  }

  public CubeColumn getColumnByName(String column) {
    return getExpressionByName(column);
  }

  /**
   * Alters the expression if already existing or just adds if it is new expression.
   *
   * @param expr
   * @throws HiveException
   */
  public void alterExpression(ExprColumn expr) throws HiveException {
    if (expr == null) {
      throw new NullPointerException("Cannot add null expression");
    }

    // Replace measure if already existing
    if (exprMap.containsKey(expr.getName().toLowerCase())) {
      expressions.remove(getExpressionByName(expr.getName()));
      LOG.info("Replacing expression " + getExpressionByName(expr.getName()) + " with " + expr);
    }

    expressions.add(expr);
    exprMap.put(expr.getName().toLowerCase(), expr);
    MetastoreUtil.addNameStrings(getProperties(), MetastoreUtil.getExpressionListKey(getName()), expressions);
    expr.addProperties(getProperties());
  }

  /**
   * Remove the measure with name specified
   *
   * @param exprName
   */
  public void removeExpression(String exprName) {
    if (exprMap.containsKey(exprName.toLowerCase())) {
      LOG.info("Removing expression " + getExpressionByName(exprName));
      expressions.remove(getExpressionByName(exprName));
      exprMap.remove(exprName.toLowerCase());
      MetastoreUtil.addNameStrings(getProperties(), MetastoreUtil.getExpressionListKey(getName()), expressions);
    }
  }

  public Set<String> getExpressionNames() {
    Set<String> exprNames = new HashSet<String>();
    for (ExprColumn f : getExpressions()) {
      exprNames.add(f.getName().toLowerCase());
    }
    return exprNames;
  }

  /**
   * @return the expressions
   */
  public Set<ExprColumn> getExpressions() {
    return expressions;
  }

  public Set<String> getAllFieldNames() {
    return getExpressionNames();
  }


  public void setJoinChainProperties(Set<JoinChain> chains) {
    for (JoinChain chain : chains) {
      chain.addProperties(this);
    }
  }

  /**
   * Alters the joinchain if already existing or just adds if it is new chain
   *
   * @param joinchain
   * @throws HiveException
   */
  public void alterJoinChain(JoinChain joinchain) throws HiveException {
    if (joinchain == null) {
      throw new NullPointerException("Cannot add null joinchain");
    }

    // Replace dimension if already existing
    if (chainMap.containsKey(joinchain.getName().toLowerCase())) {
      joinChains.remove(getChainByName(joinchain.getName()));
      LOG.info("Replacing joinchain " + getChainByName(joinchain.getName()) + " with " + joinchain);
    }

    joinChains.add(joinchain);
    chainMap.put(joinchain.getName().toLowerCase(), joinchain);
    MetastoreUtil.addNameStrings(getProperties(), getJoinChainListPropKey(getName()), joinChains);
    joinchain.addProperties(this);
  }

  public JoinChain getChainByName(String name) {
    Preconditions.checkNotNull(name);
    return chainMap.get(name.toLowerCase());
  }

  /**
   * Returns the property key for Cube/Dimension specific join chain list
   *
   * @param tblname
   * @return
   */
  protected abstract String getJoinChainListPropKey(String tblname);

  /**
   * Get join chains from properties
   *
   * @return
   */
  public Set<JoinChain> getJoinChains() {
    Set<JoinChain> joinChains = new HashSet<JoinChain>();
    String joinChainsStr = MetastoreUtil.getNamedStringValue(getProperties(), getJoinChainListPropKey(getName()));
    if (!StringUtils.isBlank(joinChainsStr)) {
      String[] cnames = joinChainsStr.split(",");
      for (String chainName : cnames) {
        JoinChain chain = new JoinChain(this, chainName);
        joinChains.add(chain);
      }
    }
    return joinChains;
  }

  public Set<String> getJoinChainNames() {
    Set<String> chainNames = new HashSet<String>();
    for (JoinChain f : getJoinChains()) {
      chainNames.add(f.getName().toLowerCase());
    }
    return chainNames;
  }


  /**
   * Remove the joinchain with name specified
   *
   * @param chainName
   */
  public boolean removeJoinChain(String chainName) {
    if (chainMap.containsKey(chainName.toLowerCase())) {
      joinChains.remove(getChainByName(chainName));
      chainMap.remove(chainName.toLowerCase());
      MetastoreUtil.addNameStrings(getProperties(), getJoinChainListPropKey(getName()), joinChains);
      return true;
    }
    return false;
  }

}
