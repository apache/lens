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

import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.parse.ASTNode;

import lombok.*;

public class ExprColumn extends CubeColumn {
  public static final char EXPRESSION_DELIMITER = '|';
  public static final char EXPRESSION_SPEC_DELIMITER = ':';
  private static final String EXPRESSION_ENCODED = "true";
  private final Set<ExprSpec> expressionSet = new LinkedHashSet<>();
  private List<ASTNode> astNodeList = new ArrayList<>();
  private final String type;
  private boolean hasHashCode = false;
  private int hashCode;

  // for backward compatibility
  public ExprColumn(FieldSchema column, String displayString, String expression) throws LensException {
    this(column, displayString, new ExprSpec(expression, null, null));
  }

  public ExprColumn(FieldSchema column, String displayString,
                    ExprSpec... expressions) throws LensException {
    this(column, displayString, new HashMap<String, String>(), expressions);
  }

  public ExprColumn(FieldSchema column, String displayString, Map<String, String> tags,
                    ExprSpec... expressions) throws LensException {
    super(column.getName(), column.getComment(), displayString, null, null, 0.0, tags);

    if (expressions == null || expressions.length == 0) {
      throw new IllegalArgumentException("No expressions specified for column " + column.getName());
    }

    for (int i = 0; i < expressions.length; i++) {
      ExprSpec e = expressions[i];
      if (StringUtils.isBlank(e.getExpr())) {
        throw new IllegalArgumentException(
          "No expression string specified for column " + column.getName() + " at index:" + i);
      }
      if (e.getStartTime() != null && e.getEndTime() != null) {
        if (e.getStartTime().after(e.getEndTime())) {
          throw new IllegalArgumentException("Start time is after end time for column " + column.getName()
            + " for expression at index:" + i + " for " + e.getExpr());
        }
      }
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
      String[] exprSpecStrs = StringUtils.splitPreserveAllTokens(e, EXPRESSION_SPEC_DELIMITER);
      try {
        String decodedExpr =
          isExpressionBase64Encoded ? new String(Base64.decodeBase64(exprSpecStrs[0]), "UTF-8") : exprSpecStrs[0];
        ExprSpec exprSpec = new ExprSpec();
        exprSpec.expr = decodedExpr;
        if (exprSpecStrs.length > 1) {
          // start time and end time serialized
          if (StringUtils.isNotBlank(exprSpecStrs[1])) {
            // start time available
            exprSpec.startTime = getDate(exprSpecStrs[1]);
          }
          if (exprSpecStrs.length > 2) {
            if (StringUtils.isNotBlank(exprSpecStrs[2])) {
              // end time available
              exprSpec.endTime = getDate(exprSpecStrs[2]);
            }
          }
        }
        expressionSet.add(exprSpec);
      } catch (UnsupportedEncodingException e1) {
        throw new IllegalArgumentException("Error decoding expression for expression column "
          + name + " encoded value=" + e);
      }
    }

    this.type = props.get(MetastoreUtil.getExprTypePropertyKey(getName()));
  }

  @NoArgsConstructor
  @ToString(exclude = {"astNode", "hasHashCode", "hashCode"})
  public static class ExprSpec {
    @Getter
    @NonNull
    private String expr;
    @Getter
    private Date startTime;
    @Getter
    private Date endTime;

    private transient ASTNode astNode;
    private boolean hasHashCode = false;
    private transient int hashCode;

    public ExprSpec(@NonNull String expr, Date startTime, Date endTime) throws LensException {
      this.expr = expr;
      this.startTime = startTime;
      this.endTime = endTime;
      // validation
      initASTNode();
    }

    private synchronized void initASTNode() throws LensException {
      if (astNode == null) {
        if (StringUtils.isNotBlank(expr)) {
          astNode = MetastoreUtil.parseExpr(getExpr());
        }
      }
    }

    private ASTNode getASTNode() throws LensException {
      initASTNode();
      return astNode;
    }

    public ASTNode copyASTNode() throws LensException {
      return MetastoreUtil.copyAST(getASTNode());
    }
    @Override
    public int hashCode() {
      if (!hasHashCode) {
        final int prime = 31;
        int result = 1;
        ASTNode astNode;
        try {
          astNode = getASTNode();
        } catch (LensException e) {
          throw new IllegalArgumentException(e);
        }
        if (astNode != null) {
          String exprNormalized = HQLParser.getString(astNode);
          result = prime * result + exprNormalized.hashCode();
        }
        result = prime * result + ((getStartTime() == null) ? 0 : COLUMN_TIME_FORMAT.get().format(
          getStartTime()).hashCode());
        result = prime * result + ((getEndTime() == null) ? 0 : COLUMN_TIME_FORMAT.get().format(
          getEndTime()).hashCode());
        hashCode = result;
        hasHashCode = true;
      }
      return hashCode;
    }
  }

  /**
   * Returns the first expression.
   *
   * @return the expression
   */
  public String getExpr() {
    return expressionSet.iterator().next().getExpr();
  }

  public String getType() {
    return type;
  }

  @Override
  public void addProperties(Map<String, String> props) {
    super.addProperties(props);

    String[] encodedExpressions = new String[expressionSet.size()];
    StringBuilder exprSpecBuilder = new StringBuilder();
    int i = 0;
    for (ExprSpec es : expressionSet) {
      String expression = es.getExpr();
      try {
        exprSpecBuilder.append(Base64.encodeBase64String(expression.getBytes("UTF-8")));
        exprSpecBuilder.append(EXPRESSION_SPEC_DELIMITER);
        if (es.getStartTime() != null) {
          exprSpecBuilder.append(COLUMN_TIME_FORMAT.get().format(es.getStartTime()));
        }
        exprSpecBuilder.append(EXPRESSION_SPEC_DELIMITER);
        if (es.getEndTime() != null) {
          exprSpecBuilder.append(COLUMN_TIME_FORMAT.get().format(es.getEndTime()));
        }
        // encoded expression contains the Base64 encoded expression, start time and end time.
        encodedExpressions[i] = exprSpecBuilder.toString();
        exprSpecBuilder.setLength(0);
        i++;
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

      for (ExprSpec exprSpec : expressionSet) {
        result = prime * result + exprSpec.hashCode();
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
    List<ASTNode> myExpressions, otherExpressions;
    try {
      myExpressions = getExpressionASTList();
    } catch (LensException e) {
      throw new IllegalArgumentException(e);
    }
    try {
      otherExpressions = other.getExpressionASTList();
    } catch (LensException e) {
      throw new IllegalArgumentException(e);
    }
    for (int i = 0; i < myExpressions.size(); i++) {
      if (!HQLParser.equalsAST(myExpressions.get(i), otherExpressions.get(i))) {
        return false;
      }
    }
    // compare start and end times for expressions
    Iterator<ExprSpec> thisIter = this.expressionSet.iterator();
    Iterator<ExprSpec> otherIter = other.expressionSet.iterator();
    while (thisIter.hasNext() && otherIter.hasNext()) {
      ExprSpec thisES = thisIter.next();
      ExprSpec otherES = otherIter.next();
      if (!equalDates(thisES.getStartTime(), otherES.getStartTime())) {
        return false;
      }
      if (!equalDates(thisES.getEndTime(), otherES.getEndTime())) {
        return false;
      }
    }
    if (thisIter.hasNext() != otherIter.hasNext()) {
      return false;
    }
    return true;
  }

  private boolean equalDates(Date d1, Date d2) {
    if (d1 == null) {
      if (d2 != null) {
        return false;
      }
    } else if (d2 == null) {
      return false;
    } else if (!COLUMN_TIME_FORMAT.get().format(d1).equals(COLUMN_TIME_FORMAT.get().format(
      d2))) {
      return false;
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
   */
  public ASTNode getAst() throws LensException {
    return getExpressionASTList().get(0);
  }

  public List<ASTNode> getExpressionASTList() throws LensException {
    synchronized (expressionSet) {
      if (astNodeList.isEmpty()) {
        for (ExprSpec expr : expressionSet) {
          astNodeList.add(expr.copyASTNode());
        }
      }
    }
    return astNodeList;
  }

  private Set<ExprSpec> getAllExpressions() {
    return expressionSet;
  }

  private final Set<String> cachedExpressionStrings = new LinkedHashSet<String>();

  /**
   * Get immutable view of this column's expression strings
   *
   * @return
   */
  public Collection<String> getExpressions() {
    if (cachedExpressionStrings.isEmpty()) {
      synchronized (expressionSet) {
        for (ExprSpec es : expressionSet) {
          cachedExpressionStrings.add(es.getExpr());
        }
      }
    }
    return Collections.unmodifiableSet(cachedExpressionStrings);
  }

  /**
   * Get immutable view of this column's expression full spec
   *
   * @return
   */
  public Collection<ExprSpec> getExpressionSpecs() {
    return Collections.unmodifiableSet(expressionSet);
  }

  /**
   * Add an expression to existing set of expressions for this column
   *
   * @param expression
   * @throws LensException
   */
  public void addExpression(ExprSpec expression) throws LensException {
    if (expression == null || expression.getExpr().isEmpty()) {
      throw new IllegalArgumentException("Empty expression not allowed");
    }

    // Validate if expression can be correctly parsed
    MetastoreUtil.parseExpr(expression.getExpr());
    synchronized (expressionSet) {
      expressionSet.add(expression);
    }
    astNodeList = null;
    hasHashCode = false;
  }

  /**
   * Remove an expression from the set of expressions of this column
   *
   * @param expression
   * @return
   */
  public boolean removeExpression(String expression) {
    if (expression == null || expression.isEmpty()) {
      throw new IllegalArgumentException("Empty expression not allowed");
    }
    boolean removed = false;
    synchronized (expressionSet) {
      Iterator<ExprSpec> it = expressionSet.iterator();
      while (it.hasNext()) {
        if (it.next().getExpr().equals(expression)) {
          it.remove();
          removed = true;
          break;
        }
      }
    }
    if (removed) {
      astNodeList = null;
      hasHashCode = false;
    }
    return removed;
  }

}
