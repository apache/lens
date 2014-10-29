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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * Abstract table with expressions
 */
public abstract class AbstractBaseTable extends AbstractCubeTable {
  private final Set<ExprColumn> expressions;
  private static final List<FieldSchema> columns = new ArrayList<FieldSchema>();
  private final Map<String, ExprColumn> exprMap;

  static {
    columns.add(new FieldSchema("dummy", "string", "dummy column"));
  }

  public AbstractBaseTable(String name, Set<ExprColumn> exprs, Map<String, String> properties, double weight) {
    super(name, columns, properties, weight);

    exprMap = new HashMap<String, ExprColumn>();
    if (exprs == null) {
      this.expressions = new HashSet<ExprColumn>();
    } else {
      this.expressions = exprs;
    }

    for (ExprColumn expr : expressions) {
      exprMap.put(expr.getName().toLowerCase(), expr);
    }
  }

  public AbstractBaseTable(Table tbl) {
    super(tbl);
    this.expressions = getExpressions(getName(), getProperties());
    exprMap = new HashMap<String, ExprColumn>();
    for (ExprColumn expr : expressions) {
      exprMap.put(expr.getName().toLowerCase(), expr);
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
    return true;
  }

  public ExprColumn getExpressionByName(String exprName) {
    return exprMap.get(exprName == null ? exprName : exprName.toLowerCase());
  }

  public CubeColumn getColumnByName(String column) {
    return getExpressionByName(column);
  }

  /**
   * Alters the expression if already existing or just adds if it is new
   * expression.
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
   * @param msrName
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

}
