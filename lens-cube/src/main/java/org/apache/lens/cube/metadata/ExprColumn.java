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

import java.io.UnsupportedEncodingException;
import java.util.*;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.lens.cube.parse.HQLParser;

public class ExprColumn extends CubeColumn {
  public static final char EXPRESSION_DELIMITER = '|';
  private static final String EXPRESSION_ENCODED = "true";
  private final Set<String> expressionSet = new LinkedHashSet<String>();
  private List<ASTNode> astNodeList;
  private final String type;
  private boolean hasHashCode = false;
  private int hashCode;

  public ExprColumn(FieldSchema column, String displayString, String ... expressions) throws ParseException {
    super(column.getName(), column.getComment(), displayString, null, null, 0.0);

    if (expressions == null || expressions.length == 0) {
      throw new IllegalArgumentException("No expression specified for column " + column.getName());
    }

    for (String e : expressions) {
      expressionSet.add(e);
    }

    this.type = column.getType();
    assert (getAst() != null);
  }

  public ExprColumn(String name, Map<String, String> props) {
    super(name, props);

    String serializedExpressions = props.get(MetastoreUtil.getExprColumnKey(getName()));
    String[] expressions = StringUtils.split(serializedExpressions, EXPRESSION_DELIMITER);

    if (expressions.length == 0) {
      throw new IllegalArgumentException("No expressions found for column "
        + name + " property val=" + serializedExpressions);
    }

    boolean isExpressionBase64Encoded =
      EXPRESSION_ENCODED.equals(props.get(MetastoreUtil.getExprEncodingPropertyKey(getName())));

    for (String e : expressions) {
      try {
        String decodedExpr = isExpressionBase64Encoded ? new String(Base64.decodeBase64(e), "UTF-8") : e;
        expressionSet.add(decodedExpr);
      } catch (UnsupportedEncodingException e1) {
        throw new IllegalArgumentException("Error decoding expression for expression column "
          + name + " encoded value=" + e);
      }
    }

    this.type = props.get(MetastoreUtil.getExprTypePropertyKey(getName()));
  }

  /**
   * @return the expression
   */
  public String getExpr() {
    return expressionSet.iterator().next();
  }

  public String getType() {
    return type;
  }

  @Override
  public void addProperties(Map<String, String> props) {
    super.addProperties(props);

    String[] encodedExpressions = expressionSet.toArray(new String[expressionSet.size()]);
    for (int i = 0; i < encodedExpressions.length; i++) {
      String expression = encodedExpressions[i];
      try {
        encodedExpressions[i] = Base64.encodeBase64String(expression.getBytes("UTF-8"));
      } catch (UnsupportedEncodingException e) {
        throw new IllegalArgumentException("Failed to encode expression " + expression);
      }
    }

    String serializedExpressions = StringUtils.join(encodedExpressions, EXPRESSION_DELIMITER);
    props.put(MetastoreUtil.getExprColumnKey(getName()) + ".base64", EXPRESSION_ENCODED);
    props.put(MetastoreUtil.getExprColumnKey(getName()), serializedExpressions);
    props.put(MetastoreUtil.getExprTypePropertyKey(getName()), type);
  }

  @Override
  public int hashCode() {
    if (!hasHashCode) {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((getType() == null) ? 0 : getType().toLowerCase().hashCode());

      for (ASTNode exprNode : getExpressionASTList()) {
        String exprNormalized = HQLParser.getString(exprNode);
        result = prime * result + exprNormalized.hashCode();
      }

      hashCode = result;
      hasHashCode = true;
    }
    return hashCode;
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
    if (this.getAllExpressions() == null) {
      if (other.getAllExpressions() != null) {
        return false;
      }
    }

    if (expressionSet.size() != other.expressionSet.size()) {
      return false;
    }
    // Compare expressions for both - compare ASTs
    List<ASTNode> myExpressions = getExpressionASTList();
    List<ASTNode> otherExpressions = other.getExpressionASTList();

    for (int i = 0; i < myExpressions.size(); i++) {
      if (!HQLParser.equalsAST(myExpressions.get(i), otherExpressions.get(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    String str = super.toString();
    str += "#type:" + type;
    str += "#expr:" + expressionSet.toString();
    return str;
  }

  /**
   * Get the AST corresponding to the expression
   * 
   * @return the ast
   * @throws ParseException
   */
  public ASTNode getAst() throws ParseException {
    return getExpressionASTList().get(0);
  }

  public List<ASTNode> getExpressionASTList() {
    if (astNodeList == null) {
      astNodeList = new ArrayList<ASTNode>(expressionSet.size());
      for (String expr : expressionSet) {
        try {
          astNodeList.add(HQLParser.parseExpr(expr));
        } catch (ParseException e) {
          // Should not throw exception since expr should have been validated when it was added
          throw new IllegalStateException("Expression can't be parsed: " + expr, e);
        }
      }
    }
    return astNodeList;
  }

  private Set<String> getAllExpressions() {
    return expressionSet;
  }

  /**
   * Get immutable view of this column's expressions
   * @return
   */
  public Collection<String> getExpressions() {
    return Collections.unmodifiableSet(expressionSet);
  }

  /**
   * Add an expression to existing set of expressions for this column
   * @param expression
   * @throws ParseException
   */
  public void addExpression(String expression) throws ParseException {
    if (expression == null || expression.isEmpty()) {
      throw new IllegalArgumentException("Empty expression not allowed");
    }

    // Validate if expression can be correctly parsed
    HQLParser.parseExpr(expression);
    expressionSet.add(expression);
    astNodeList = null;
    hasHashCode = false;
  }

  /**
   * Remove an expression from the set of expressions of this column
   * @param expression
   * @return
   */
  public boolean removeExpression(String expression) {
    if (expression == null || expression.isEmpty()) {
      throw new IllegalArgumentException("Empty expression not allowed");
    }
    boolean removed = expressionSet.remove(expression);
    if (removed) {
      astNodeList = null;
      hasHashCode = false;
    }
    return removed;
  }

}
