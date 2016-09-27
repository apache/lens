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

import static org.apache.lens.cube.error.LensCubeErrorCode.COULD_NOT_PARSE_EXPRESSION;
import static org.apache.lens.cube.error.LensCubeErrorCode.SYNTAX_ERROR;

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.*;

import org.antlr.runtime.CommonToken;
import org.antlr.runtime.tree.Tree;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public final class HQLParser {

  private HQLParser() {

  }

  public static final Pattern P_WSPACE = Pattern.compile("\\s+");

  public static boolean isTableColumnAST(ASTNode astNode) {
    return !(astNode == null || astNode.getChildren() == null || astNode.getChildCount() != 2) && astNode.getChild(0)
      .getType() == HiveParser.TOK_TABLE_OR_COL && astNode.getChild(1).getType() == HiveParser.Identifier;
  }

  public static boolean isPrimitiveBooleanExpression(ASTNode ast) {
    return HQLParser.FILTER_OPERATORS.contains(ast.getType());
  }

  public static boolean isPrimitiveBooleanFunction(ASTNode ast) {
    if (ast.getType() == TOK_FUNCTION) {
      if (ast.getChild(0).getText().equals("in")) {
        return true;
      }
    }
    return false;
  }
  public static ASTNode getDotAST(String tableAlias, String fieldAlias) {
    ASTNode child = new ASTNode(new CommonToken(DOT, "."));
    child.addChild(new ASTNode(new CommonToken(TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL")));
    child.getChild(0).addChild(new ASTNode(new CommonToken(Identifier, tableAlias)));
    child.addChild(new ASTNode(new CommonToken(Identifier, fieldAlias)));
    return child;
  }

  public interface ASTNodeVisitor {
    void visit(TreeNode node) throws LensException;
  }

  public static class TreeNode {
    final TreeNode parent;
    final ASTNode node;

    public TreeNode(TreeNode parent, ASTNode node) {
      this.parent = parent;
      this.node = node;
    }

    public TreeNode getParent() {
      return parent;
    }

    public ASTNode getNode() {
      return node;
    }
  }

  public static final Set<Integer> BINARY_OPERATORS;
  public static final Set<Integer> N_ARY_OPERATORS;
  public static final Set<Integer> FILTER_OPERATORS;
  public static final Set<Integer> ARITHMETIC_OPERATORS;
  public static final Set<Integer> UNARY_OPERATORS;
  public static final Set<Integer> PRIMITIVE_TYPES;

  static {
    HashSet<Integer> ops = new HashSet<>();
    ops.add(DOT);
    ops.add(KW_AND);
    ops.add(KW_OR);
    ops.add(EQUAL);
    ops.add(EQUAL_NS);
    ops.add(NOTEQUAL);
    ops.add(GREATERTHAN);
    ops.add(GREATERTHANOREQUALTO);
    ops.add(LESSTHAN);
    ops.add(LESSTHANOREQUALTO);
    ops.add(PLUS);
    ops.add(MINUS);
    ops.add(STAR);
    ops.add(DIVIDE);
    ops.add(MOD);
    ops.add(KW_LIKE);
    ops.add(KW_RLIKE);
    ops.add(KW_REGEXP);
    ops.add(AMPERSAND);
    ops.add(BITWISEOR);
    ops.add(BITWISEXOR);

    BINARY_OPERATORS = Collections.unmodifiableSet(ops);
    N_ARY_OPERATORS = Collections.unmodifiableSet(Sets.newHashSet(KW_AND, KW_OR, PLUS, STAR,
      AMPERSAND, BITWISEOR, BITWISEXOR));

    ARITHMETIC_OPERATORS = new HashSet<>();
    ARITHMETIC_OPERATORS.add(PLUS);
    ARITHMETIC_OPERATORS.add(MINUS);
    ARITHMETIC_OPERATORS.add(STAR);
    ARITHMETIC_OPERATORS.add(DIVIDE);
    ARITHMETIC_OPERATORS.add(MOD);

    HashSet<Integer> unaryOps = new HashSet<>();
    unaryOps.add(KW_NOT);
    unaryOps.add(TILDE);
    UNARY_OPERATORS = Collections.unmodifiableSet(unaryOps);

    HashSet<Integer> primitiveTypes = new HashSet<>();
    primitiveTypes.add(TOK_TINYINT);
    primitiveTypes.add(TOK_SMALLINT);
    primitiveTypes.add(TOK_INT);
    primitiveTypes.add(TOK_BIGINT);
    primitiveTypes.add(TOK_BOOLEAN);
    primitiveTypes.add(TOK_FLOAT);
    primitiveTypes.add(TOK_DOUBLE);
    primitiveTypes.add(TOK_DATE);
    primitiveTypes.add(TOK_DATETIME);
    primitiveTypes.add(TOK_TIMESTAMP);
    primitiveTypes.add(TOK_STRING);
    primitiveTypes.add(TOK_BINARY);
    primitiveTypes.add(TOK_DECIMAL);
    primitiveTypes.add(TOK_VARCHAR);
    primitiveTypes.add(TOK_CHAR);
    PRIMITIVE_TYPES = Collections.unmodifiableSet(primitiveTypes);

    FILTER_OPERATORS = Sets.newHashSet(GREATERTHAN, GREATERTHANOREQUALTO, LESSTHAN, LESSTHANOREQUALTO, EQUAL,
      EQUAL_NS, NOTEQUAL);
  }

  public static ASTNode parseHQL(String query, HiveConf conf) throws LensException {
    ParseDriver driver = new ParseDriver();
    ASTNode tree = null;
    Context ctx = null;
    try {
      ctx = new Context(conf);
      tree = driver.parse(query, ctx);
      tree = ParseUtils.findRootNonNullToken(tree);
    } catch (ParseException e) {
      throw new LensException(SYNTAX_ERROR.getLensErrorInfo(), e, e.getMessage());
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (ctx != null) {
        try {
          ctx.clear();
        } catch (IOException e) {
          // ignoring exception in clear
        }
      }
    }
    return tree;
  }

  public static ASTNode parseExpr(String expr) throws LensException {
    ParseDriver driver = new ParseDriver();
    ASTNode tree;
    try {
      tree = driver.parseExpression(expr);
    } catch (ParseException e) {
      throw new LensException(COULD_NOT_PARSE_EXPRESSION.getLensErrorInfo(), e, e.getMessage());
    }
    return ParseUtils.findRootNonNullToken(tree);
  }

  public static void printAST(ASTNode node) {
    try {
      printAST(getHiveTokenMapping(), node, 0, 0);
    } catch (Exception e) {
      log.error("Error in printing AST.", e);
    }
    System.out.println();
  }

  /**
   * Debug function for printing query AST to stdout
   *
   * @param tokenMapping token mapping
   * @param node         node
   * @param level        level
   * @param child        child
   */
  public static void printAST(Map<Integer, String> tokenMapping, ASTNode node, int level, int child) {
    if (node == null || node.isNil()) {
      return;
    }

    for (int i = 0; i < level; i++) {
      System.out.print("  ");
    }

    System.out.print(node.getText() + " [" + tokenMapping.get(node.getType()) + "]");
    System.out.print(" (l" + level + "c" + child + "p" + node.getCharPositionInLine() + ")");

    if (node.getChildCount() > 0) {
      System.out.println(" {");

      for (int i = 0; i < node.getChildCount(); i++) {
        Tree tree = node.getChild(i);
        if (tree instanceof ASTNode) {
          printAST(tokenMapping, (ASTNode) tree, level + 1, i + 1);
        } else {
          System.out.println("NON ASTNode");
        }
        System.out.println();
      }

      for (int i = 0; i < level; i++) {
        System.out.print("  ");
      }

      System.out.print("}");

    } else {
      System.out.print('$');
    }
  }

  public static Map<Integer, String> getHiveTokenMapping() throws Exception {
    Map<Integer, String> mapping = new HashMap<>();

    for (Field f : HiveParser.class.getFields()) {
      if (f.getType() == int.class) {
        Integer tokenId = f.getInt(null);
        String token = f.getName();
        mapping.put(tokenId, token);
      }
    }

    return mapping;
  }

  /**
   * Find a node in the tree rooted at root, given the path of type of tokens from the root's children to the desired
   * node
   *
   * @param root node from which searching is to be started
   * @param path starts at the level of root's children
   * @return Node if found, else null
   */
  public static ASTNode findNodeByPath(ASTNode root, int... path) {
    for (int i = 0; i < path.length; i++) {
      int type = path[i];
      boolean hasChildWithType = false;

      for (int j = 0; j < root.getChildCount(); j++) {
        ASTNode node = (ASTNode) root.getChild(j);
        if (node.getType() == type) {
          hasChildWithType = true;
          root = node;
          // If this is the last type in path, return this node
          if (i == path.length - 1) {
            return root;
          } else {
            // Go to next level
            break;
          }
        }
      }

      if (!hasChildWithType) {
        // No path from this level
        break;
      }
    }

    return null;
  }

  /**
   * Breadth first traversal of AST
   *
   * @param root      node from where to start bft
   * @param visitor   action to take on each visit
   * @throws LensException
   */
  public static void bft(ASTNode root, ASTNodeVisitor visitor) throws LensException {
    if (root == null) {
      throw new NullPointerException("Root cannot be null");
    }

    if (visitor == null) {
      throw new NullPointerException("Visitor cannot be null");
    }
    Queue<TreeNode> queue = new LinkedList<>();
    queue.add(new TreeNode(null, root));

    while (!queue.isEmpty()) {
      TreeNode node = queue.poll();
      visitor.visit(node);
      ASTNode astNode = node.getNode();
      for (int i = 0; i < astNode.getChildCount(); i++) {
        queue.offer(new TreeNode(node, (ASTNode) astNode.getChild(i)));
      }
    }
  }

  static boolean hasSpaces(String text) {
    return P_WSPACE.matcher(text).find();
  }

  public static void toInfixString(ASTNode root, StringBuilder buf) {
    toInfixString(root, buf, AppendMode.LOWER_CASE);
  }

  /**
   * Recursively reconstruct query string given a query AST
   *
   * @param root root node
   * @param buf  preallocated builder where the reconstructed string will be written
   */
  public static void toInfixString(ASTNode root, StringBuilder buf, AppendMode appendMode) {
    if (root == null) {
      return;
    }
    int rootType = root.getType();
    String rootText = root.getText();
    // Operand, print contents
    if (Identifier == rootType || Number == rootType || StringLiteral == rootType || KW_TRUE == rootType
      || KW_FALSE == rootType || KW_FORMATTED == rootType || KW_EXTENDED == rootType || KW_DEPENDENCY == rootType) {
      // StringLiterals should not be lower cased.
      if (StringLiteral == rootType) {
        buf.append(rootText);
      } else if (KW_TRUE == rootType) {
        buf.append(" true ");
      } else if (KW_FALSE == rootType) {
        buf.append(" false ");
      } else if (Identifier == rootType && TOK_SELEXPR == root.getParent().getType()) {
        // back quote column alias in all cases. This is required since some alias values can match DB keywords
        // (example : year as alias) and in such case queries can fail on certain DBs if the alias in not back quoted
        buf.append(" as `").append(rootText).append("`");
      } else {
        buf.append(rootText == null ? "" : appendMode.convert(rootText));
      }

    } else if (TOK_ALLCOLREF == rootType) {
      if (root.getChildCount() > 0) {
        for (int i = 0; i < root.getChildCount(); i++) {
          toInfixString((ASTNode) root.getChild(i), buf, appendMode);
        }
        buf.append(".");
      }
      buf.append("*");
    } else if (TOK_FUNCTIONSTAR == rootType) {
      if (root.getChildCount() > 0) {
        for (int i = 0; i < root.getChildCount(); i++) {
          toInfixString((ASTNode) root.getChild(i), buf, appendMode);
        }
      }
      buf.append("(*)");
    } else if (UNARY_OPERATORS.contains(rootType)) {
      if (KW_NOT == rootType) {
        // Check if this is actually NOT IN
        if (findNodeByPath(root, TOK_FUNCTION, KW_IN) == null) {
          buf.append(" not ");
        }
      } else if (TILDE == rootType) {
        buf.append(" ~");
      }

      for (int i = 0; i < root.getChildCount(); i++) {
        toInfixString((ASTNode) root.getChild(i), buf, appendMode);
      }

    } else if (BINARY_OPERATORS.contains(rootType)) {
      boolean surround = true;
      if (N_ARY_OPERATORS.contains(rootType)
        && (root.getParent() == null || rootType == root.getParent().getType())) {
        surround = false;
      }
      if (surround) {
        buf.append("(");
      }
      if (MINUS == rootType && root.getChildCount() == 1) {
        // If minus has only one child, then it's a unary operator.
        // Add Operator name first
        buf.append(appendMode.convert(rootText));
        // Operand
        toInfixString((ASTNode) root.getChild(0), buf, appendMode);
      } else {
        // Left operand
        toInfixString((ASTNode) root.getChild(0), buf, appendMode);
        // Operator name
        if (rootType != DOT) {
          buf.append(' ').append(appendMode.convert(rootText)).append(' ');
        } else {
          buf.append(appendMode.convert(rootText));
        }
        // Right operand
        toInfixString((ASTNode) root.getChild(1), buf, appendMode);
      }
      if (surround) {
        buf.append(")");
      }
    } else if (LSQUARE == rootType) {
      // square brackets for array and map types
      toInfixString((ASTNode) root.getChild(0), buf, appendMode);
      buf.append("[");
      toInfixString((ASTNode) root.getChild(1), buf, appendMode);
      buf.append("]");
    } else if (PRIMITIVE_TYPES.contains(rootType)) {
      if (rootType == TOK_TINYINT) {
        buf.append("tinyint");
      } else if (rootType == TOK_SMALLINT) {
        buf.append("smallint");
      } else if (rootType == TOK_INT) {
        buf.append("int");
      } else if (rootType == TOK_BIGINT) {
        buf.append("bigint");
      } else if (rootType == TOK_BOOLEAN) {
        buf.append("boolean");
      } else if (rootType == TOK_FLOAT) {
        buf.append("float");
      } else if (rootType == TOK_DOUBLE) {
        buf.append("double");
      } else if (rootType == TOK_DATE) {
        buf.append("date");
      } else if (rootType == TOK_DATETIME) {
        buf.append("datetime");
      } else if (rootType == TOK_TIMESTAMP) {
        buf.append("timestamp");
      } else if (rootType == TOK_STRING) {
        buf.append("string");
      } else if (rootType == TOK_BINARY) {
        buf.append("binary");
      } else if (rootType == TOK_DECIMAL) {
        buf.append("decimal");
        if (root.getChildCount() >= 1) {
          buf.append("(").append(root.getChild(0).getText());
          if (root.getChildCount() == 2) {
            buf.append(",").append(root.getChild(1).getText());
          }
          buf.append(")");
        }
      } else if (rootType == TOK_VARCHAR) {
        buf.append("varchar");
        if (root.getChildCount() >= 1) {
          buf.append("(").append(root.getChild(0).getText()).append(")");
        }
      } else if (rootType == TOK_CHAR) {
        buf.append("char");
        if (root.getChildCount() >= 1) {
          buf.append("(").append(root.getChild(0).getText()).append(")");
        }
      } else {
        buf.append(rootText);
      }
    } else if (TOK_FUNCTION == root.getType()) {
      // Handle UDFs, conditional operators.
      functionString(root, buf, appendMode);

    } else if (TOK_FUNCTIONDI == rootType) {
      // Distinct is a different case.
      String fname = root.getChild(0).getText();

      buf.append(appendMode.convert(fname)).append("( distinct ");

      // Arguments to distinct separated by comma
      for (int i = 1; i < root.getChildCount(); i++) {
        toInfixString((ASTNode) root.getChild(i), buf, appendMode);
        if (i != root.getChildCount() - 1) {
          buf.append(", ");
        }
      }
      buf.append(")");

    } else if (TOK_TABSORTCOLNAMEDESC == rootType || TOK_TABSORTCOLNAMEASC == rootType) {
      for (int i = 0; i < root.getChildCount(); i++) {
        StringBuilder orderByCol = new StringBuilder();
        toInfixString((ASTNode) root.getChild(i), orderByCol, appendMode);
        String colStr = orderByCol.toString().trim();
        if (colStr.startsWith("(") && colStr.endsWith(")")) {
          colStr = colStr.substring(1, colStr.length() - 1);
        }
        buf.append(colStr);
      }
      buf.append(rootType == TOK_TABSORTCOLNAMEDESC ? " desc" : " asc");
    } else if (TOK_SELECT == rootType || TOK_ORDERBY == rootType || TOK_GROUPBY == rootType) {
      for (int i = 0; i < root.getChildCount(); i++) {
        toInfixString((ASTNode) root.getChild(i), buf, appendMode);
        if (i != root.getChildCount() - 1) {
          buf.append(", ");
        }
      }

    } else if (TOK_SELECTDI == rootType) {
      buf.append(" distinct ");
      for (int i = 0; i < root.getChildCount(); i++) {
        toInfixString((ASTNode) root.getChild(i), buf, appendMode);
        if (i != root.getChildCount() - 1) {
          buf.append(", ");
        }
      }

    } else if (TOK_DIR == rootType) {
      StringBuilder sb = new StringBuilder();
      boolean local = false;
      for (int i = 0; i < root.getChildCount(); i++) {

        if (root.getChild(i).getType() == KW_LOCAL) {
          local = true;
        } else {
          toInfixString((ASTNode) root.getChild(i), sb, appendMode);
        }
      }
      buf.append(local ? " local": "").append(" directory ").append(sb);
    } else if (TOK_TAB == rootType) {
      buf.append(" table ");
      for (int i = 0; i < root.getChildCount(); i++) {
        toInfixString((ASTNode) root.getChild(i), buf, appendMode);
      }

    } else {
      if (root.getChildCount() > 0) {
        for (int i = 0; i < root.getChildCount(); i++) {
          toInfixString((ASTNode) root.getChild(i), buf, appendMode);
        }
      } else {
        // for other types which are not handled above
        buf.append(rootText);
      }
    }
  }

  // Get string representation of a function node in query AST
  private static void functionString(ASTNode root, StringBuilder buf, AppendMode appendMode) {
    // special handling for CASE udf
    if (findNodeByPath(root, KW_CASE) != null) {
      buf.append("case ");
      toInfixString((ASTNode) root.getChild(1), buf, appendMode);
      // each of the conditions
      ArrayList<Node> caseChildren = root.getChildren();
      int from = 2;
      int nchildren = caseChildren.size();
      int to = nchildren % 2 == 1 ? nchildren - 1 : nchildren;

      for (int i = from; i < to; i += 2) {
        buf.append(" when ");
        toInfixString((ASTNode) caseChildren.get(i), buf, appendMode);
        buf.append(" then ");
        toInfixString((ASTNode) caseChildren.get(i + 1), buf, appendMode);
      }

      // check if there is an ELSE node
      if (nchildren % 2 == 1) {
        buf.append(" else ");
        toInfixString((ASTNode) caseChildren.get(nchildren - 1), buf, appendMode);
      }

      buf.append(" end");

    } else if (findNodeByPath(root, KW_WHEN) != null) {
      // 2nd form of case statement

      buf.append("case ");
      // each of the conditions
      ArrayList<Node> caseChildren = root.getChildren();
      int from = 1;
      int nchildren = caseChildren.size();
      int to = nchildren % 2 == 1 ? nchildren : nchildren - 1;

      for (int i = from; i < to; i += 2) {
        buf.append(" when ");
        toInfixString((ASTNode) caseChildren.get(i), buf, appendMode);
        buf.append(" then ");
        toInfixString((ASTNode) caseChildren.get(i + 1), buf, appendMode);
      }

      // check if there is an ELSE node
      if (nchildren % 2 == 0) {
        buf.append(" else ");
        toInfixString((ASTNode) caseChildren.get(nchildren - 1), buf, appendMode);
      }

      buf.append(" end");

    } else if (findNodeByPath(root, TOK_ISNULL) != null) {
      // IS NULL operator
      toInfixString((ASTNode) root.getChild(1), buf, appendMode);
      buf.append(" is null");

    } else if (findNodeByPath(root, TOK_ISNOTNULL) != null) {
      // IS NOT NULL operator
      toInfixString((ASTNode) root.getChild(1), buf, appendMode);
      buf.append(" is not null");

    } else if (root.getChild(0).getType() == Identifier
      && ((ASTNode) root.getChild(0)).getToken().getText().equalsIgnoreCase("between")) {
      // Handle between and not in between
      ASTNode tokTrue = findNodeByPath(root, KW_TRUE);
      ASTNode tokFalse = findNodeByPath(root, KW_FALSE);
      if (tokTrue != null) {
        // NOT BETWEEN
        toInfixString((ASTNode) root.getChild(2), buf, appendMode);
        buf.append(" not between ");
        toInfixString((ASTNode) root.getChild(3), buf, appendMode);
        buf.append(" and ");
        toInfixString((ASTNode) root.getChild(4), buf, appendMode);
      } else if (tokFalse != null) {
        // BETWEEN
        toInfixString((ASTNode) root.getChild(2), buf, appendMode);
        buf.append(" between ");
        toInfixString((ASTNode) root.getChild(3), buf, appendMode);
        buf.append(" and ");
        toInfixString((ASTNode) root.getChild(4), buf, appendMode);
      }

    } else if (findNodeByPath(root, KW_IN) != null) {
      // IN operator

      toInfixString((ASTNode) root.getChild(1), buf, appendMode);

      // check if this is NOT In
      ASTNode rootParent = (ASTNode) root.getParent();
      if (rootParent != null && rootParent.getType() == KW_NOT) {
        buf.append(" not ");
      }

      buf.append(" in (");

      for (int i = 2; i < root.getChildCount(); i++) {
        toInfixString((ASTNode) root.getChild(i), buf, appendMode);
        if (i < root.getChildCount() - 1) {
          buf.append(" , ");
        }
      }

      buf.append(")");
    } else if (findNodeByPath(root, KW_CAST) != null) {
      buf.append("cast");
      toInfixString((ASTNode) root.getChild(1), buf, appendMode);
      buf.append(" as ");
      toInfixString((ASTNode) root.getChild(0), buf, appendMode);
    } else {
      int rootType = root.getChild(0).getType();
      if (PRIMITIVE_TYPES.contains(rootType)) {
        // cast expression maps to the following ast
        // KW_CAST LPAREN expression KW_AS primitiveType RPAREN -> ^(TOK_FUNCTION primitiveType expression)
        buf.append("cast(");
        toInfixString((ASTNode) root.getChild(1), buf, appendMode);
        buf.append(" as ");
        toInfixString((ASTNode) root.getChild(0), buf, appendMode);
        buf.append(")");
      } else {
        // Normal UDF
        String fname = root.getChild(0).getText();
        // Function name
        buf.append(appendMode.convert(fname)).append("(");
        // Arguments separated by comma
        for (int i = 1; i < root.getChildCount(); i++) {
          toInfixString((ASTNode) root.getChild(i), buf, appendMode);
          if (i != root.getChildCount() - 1) {
            buf.append(", ");
          }
        }
        buf.append(")");
      }
    }
  }

  public static void main(String[] args) throws Exception {
    ASTNode ast = parseHQL("select * from default_table ", new HiveConf());

    printAST(getHiveTokenMapping(), ast, 0, 0);
  }

  public static String getString(ASTNode tree, AppendMode appendMode) {
    StringBuilder buf = new StringBuilder();
    toInfixString(tree, buf, appendMode);
    return buf.toString().trim().replaceAll("\\s+", " ");
  }

  public static String getString(ASTNode tree) {
    StringBuilder buf = new StringBuilder();
    toInfixString(tree, buf);
    return buf.toString().trim();
  }

  public static String getColName(ASTNode node) {
    String colname;
    int nodeType = node.getType();
    if (nodeType == HiveParser.TOK_TABLE_OR_COL) {
      colname = node.getChild(0).getText();
    } else {
      // node in 'alias.column' format
      ASTNode colIdent = (ASTNode) node.getChild(1);
      colname = colIdent.getText();
    }

    return colname;
  }

  public static Set<String> getColsInExpr(final String tableAlias, ASTNode expr) throws LensException {
    final Set<String> colsInExpr = new HashSet<>();
    HQLParser.bft(expr, new ASTNodeVisitor() {
      @Override
      public void visit(TreeNode visited) {
        ASTNode node = visited.getNode();
        ASTNode parent = null;
        if (visited.getParent() != null) {
          parent = visited.getParent().getNode();
        }
        if (node.getToken().getType() == DOT) {
          String alias = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL, Identifier).getText().toLowerCase();
          ASTNode colIdent = (ASTNode) node.getChild(1);
          String column = colIdent.getText().toLowerCase();
          if (tableAlias.equalsIgnoreCase(alias)) {
            colsInExpr.add(column);
          }
        }
      }
    });
    return colsInExpr;
  }

  public static boolean isAggregateAST(ASTNode node) {
    int exprTokenType = node.getType();
    if (exprTokenType == HiveParser.TOK_FUNCTION || exprTokenType == HiveParser.TOK_FUNCTIONDI
      || exprTokenType == HiveParser.TOK_FUNCTIONSTAR) {
      assert (node.getChildCount() != 0);
      if (node.getChild(0).getType() == HiveParser.Identifier) {
        String functionName = BaseSemanticAnalyzer.unescapeIdentifier(node.getChild(0).getText());
        try {
          if (FunctionRegistry.getGenericUDAFResolver(functionName) != null) {
            return true;
          }
        } catch (SemanticException e) {
          log.error("Error trying to find whether {} is aggregate.", getString(node), e);
          return false;
        }
      }
    }

    return false;
  }

  public static boolean isNonAggregateFunctionAST(ASTNode node) {
    int exprTokenType = node.getType();
    if (exprTokenType == HiveParser.TOK_FUNCTION || exprTokenType == HiveParser.TOK_FUNCTIONDI
      || exprTokenType == HiveParser.TOK_FUNCTIONSTAR) {
      assert (node.getChildCount() != 0);
      if (node.getChild(0).getType() == HiveParser.Identifier) {
        String functionName = BaseSemanticAnalyzer.unescapeIdentifier(node.getChild(0).getText());
        try {
          if (FunctionRegistry.getGenericUDAFResolver(functionName) == null) {
            return true;
          }
        } catch (SemanticException e) {
          log.error("Error trying to find whether {} is udf node.", getString(node), e);
          return false;
        }
      }
    }
    return false;
  }

  /**
   * @param node an ASTNode
   * @return true when input node is a SELECT AST Node. Otherwise, false.
   */
  public static boolean isSelectASTNode(final ASTNode node) {

    Optional<Integer> astNodeType = getASTNodeType(node);
    return astNodeType.isPresent() && astNodeType.get() == HiveParser.TOK_SELECT;

  }

  /**
   * @param node an ASTNode
   * @return When node is null or token inside node is null, then Optional.absent is returned. Otherwise, an integer
   * representing ASTNodeType is returned.
   */
  private static Optional<Integer> getASTNodeType(final ASTNode node) {

    Optional<Integer> astNodeType = Optional.absent();
    if (node != null && node.getToken() != null) {
      astNodeType = Optional.of(node.getType());
    }

    return astNodeType;
  }

  public static boolean hasAggregate(ASTNode node) {
    int nodeType = node.getType();
    if (nodeType == HiveParser.TOK_TABLE_OR_COL || nodeType == HiveParser.DOT) {
      return false;
    } else {
      if (HQLParser.isAggregateAST(node)) {
        return true;
      }

      for (int i = 0; i < node.getChildCount(); i++) {
        if (hasAggregate((ASTNode) node.getChild(i))) {
          return true;
        }
      }
      return false;
    }
  }

  public static boolean equalsAST(ASTNode n1, ASTNode n2) {
    if (n1 == null && n2 != null) {
      return false;
    }

    if (n1 != null && n2 == null) {
      return false;
    }

    if (n1 == null) {
      return true;
    }

    if (n1.getType() != n2.getType()) {
      return false;
    }

    // Compare text. For literals, comparison is case sensitive
    if ((n1.getType() == StringLiteral && !StringUtils.equals(n1.getText(), n2.getText()))) {
      return false;
    }

    if (!StringUtils.equalsIgnoreCase(n1.getText(), n2.getText())) {
      return false;
    }

    // Compare children
    if (n1.getChildCount() != n2.getChildCount()) {
      return false;
    }

    for (int i = 0; i < n1.getChildCount(); i++) {
      if (!equalsAST((ASTNode) n1.getChild(i), (ASTNode) n2.getChild(i))) {
        return false;
      }
    }

    return true;
  }

  public static ASTNode leftMostChild(ASTNode node) {
    while (node.getChildren() != null) {
      node = (ASTNode) node.getChild(0);
    }
    return node;
  }
  @Data
  public static class HashableASTNode {
    private ASTNode ast;
    private int hashCode = -1;
    private boolean hashCodeComputed = false;

    public HashableASTNode(ASTNode ast) {
      this.ast = ast;
    }

    public void setAST(ASTNode ast) {
      this.ast = ast;
      hashCodeComputed = false;
    }

    public ASTNode getAST() {
      return ast;
    }

    @Override
    public int hashCode() {
      if (!hashCodeComputed) {
        hashCode = getString(ast).hashCode();
        hashCodeComputed = true;
      }
      return hashCode;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof HashableASTNode && this.hashCode() == o.hashCode() && getString(this.getAST())
        .trim().equalsIgnoreCase(getString(((HashableASTNode) o).getAST()).trim());
    }
  }

  public enum AppendMode {
    LOWER_CASE {
      @Override public String convert(String s) {
        return s.toLowerCase();
      }
    },
    DEFAULT;
    public String convert(String s) {
      return s;
    }
  }
}
