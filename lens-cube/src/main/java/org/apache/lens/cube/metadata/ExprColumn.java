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

import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.lens.cube.parse.HQLParser;

public class ExprColumn extends CubeColumn {
  private final String expr;
  private final String type;
  private ASTNode ast;

  public ExprColumn(FieldSchema column, String displayString, String expr) throws ParseException {
    super(column.getName(), column.getComment(), displayString, null, null, 0.0);
    this.expr = expr;
    this.type = column.getType();
    assert (getAst() != null);
  }

  public ExprColumn(String name, Map<String, String> props) {
    super(name, props);
    this.expr = props.get(MetastoreUtil.getExprColumnKey(getName()));
    this.type = props.get(MetastoreUtil.getExprTypePropertyKey(getName()));
  }

  /**
   * @return the expression
   */
  public String getExpr() {
    return expr;
  }

  public String getType() {
    return type;
  }

  @Override
  public void addProperties(Map<String, String> props) {
    super.addProperties(props);
    props.put(MetastoreUtil.getExprColumnKey(getName()), expr);
    props.put(MetastoreUtil.getExprTypePropertyKey(getName()), type);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((getType() == null) ? 0 : getType().toLowerCase().hashCode());
    result = prime * result + ((getExpr() == null) ? 0 : getExpr().toLowerCase().hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    ExprColumn other = (ExprColumn) obj;
    if (this.getType() == null) {
      if (other.getType() != null) {
        return false;
      }
    } else if (!this.getType().equalsIgnoreCase(other.getType())) {
      return false;
    }
    if (this.getExpr() == null) {
      if (other.getExpr() != null) {
        return false;
      }
    } else if (!this.getExpr().equalsIgnoreCase(other.getExpr())) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    String str = super.toString();
    str += "#type:" + type;
    str += "#expr:" + expr;
    return str;
  }

  /**
   * Get the AST corresponding to the expression
   * 
   * @return the ast
   * @throws ParseException
   */
  public ASTNode getAst() throws ParseException {
    if (ast == null) {
      this.ast = HQLParser.parseExpr(expr);
    }
    return ast;
  }
}
