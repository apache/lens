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

import static org.apache.hadoop.hive.ql.parse.HiveParser.AMPERSAND;
import static org.apache.hadoop.hive.ql.parse.HiveParser.BITWISEOR;
import static org.apache.hadoop.hive.ql.parse.HiveParser.BITWISEXOR;
import static org.apache.hadoop.hive.ql.parse.HiveParser.DIVIDE;
import static org.apache.hadoop.hive.ql.parse.HiveParser.DOT;
import static org.apache.hadoop.hive.ql.parse.HiveParser.EQUAL;
import static org.apache.hadoop.hive.ql.parse.HiveParser.EQUAL_NS;
import static org.apache.hadoop.hive.ql.parse.HiveParser.GREATERTHAN;
import static org.apache.hadoop.hive.ql.parse.HiveParser.GREATERTHANOREQUALTO;
import static org.apache.hadoop.hive.ql.parse.HiveParser.Identifier;
import static org.apache.hadoop.hive.ql.parse.HiveParser.KW_AND;
import static org.apache.hadoop.hive.ql.parse.HiveParser.KW_CASE;
import static org.apache.hadoop.hive.ql.parse.HiveParser.KW_DEPENDENCY;
import static org.apache.hadoop.hive.ql.parse.HiveParser.KW_EXTENDED;
import static org.apache.hadoop.hive.ql.parse.HiveParser.KW_FALSE;
import static org.apache.hadoop.hive.ql.parse.HiveParser.KW_FORMATTED;
import static org.apache.hadoop.hive.ql.parse.HiveParser.KW_IN;
import static org.apache.hadoop.hive.ql.parse.HiveParser.KW_LIKE;
import static org.apache.hadoop.hive.ql.parse.HiveParser.KW_NOT;
import static org.apache.hadoop.hive.ql.parse.HiveParser.KW_OR;
import static org.apache.hadoop.hive.ql.parse.HiveParser.KW_REGEXP;
import static org.apache.hadoop.hive.ql.parse.HiveParser.KW_RLIKE;
import static org.apache.hadoop.hive.ql.parse.HiveParser.KW_TRUE;
import static org.apache.hadoop.hive.ql.parse.HiveParser.KW_WHEN;
import static org.apache.hadoop.hive.ql.parse.HiveParser.LESSTHAN;
import static org.apache.hadoop.hive.ql.parse.HiveParser.LESSTHANOREQUALTO;
import static org.apache.hadoop.hive.ql.parse.HiveParser.LSQUARE;
import static org.apache.hadoop.hive.ql.parse.HiveParser.MINUS;
import static org.apache.hadoop.hive.ql.parse.HiveParser.MOD;
import static org.apache.hadoop.hive.ql.parse.HiveParser.NOTEQUAL;
import static org.apache.hadoop.hive.ql.parse.HiveParser.Number;
import static org.apache.hadoop.hive.ql.parse.HiveParser.PLUS;
import static org.apache.hadoop.hive.ql.parse.HiveParser.STAR;
import static org.apache.hadoop.hive.ql.parse.HiveParser.StringLiteral;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TILDE;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_ALLCOLREF;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_DIR;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_FUNCTION;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_FUNCTIONDI;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_FUNCTIONSTAR;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_GROUPBY;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_ISNOTNULL;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_ISNULL;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_LOCAL_DIR;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_ORDERBY;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_SELECT;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_SELEXPR;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_SELECTDI;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TABSORTCOLNAMEASC;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TABSORTCOLNAMEDESC;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TAB;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Pattern;

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class HQLParser {
  public static final Pattern P_WSPACE = Pattern.compile("\\s+");

  public static interface ASTNodeVisitor {
    public void visit(TreeNode node) throws SemanticException;
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
  public static final Set<Integer> ARITHMETIC_OPERATORS;
  public static final Set<Integer> UNARY_OPERATORS;

  static {
    HashSet<Integer> ops = new HashSet<Integer>();
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

    ARITHMETIC_OPERATORS = new HashSet<Integer>();
    ARITHMETIC_OPERATORS.add(PLUS);
    ARITHMETIC_OPERATORS.add(MINUS);
    ARITHMETIC_OPERATORS.add(STAR);
    ARITHMETIC_OPERATORS.add(DIVIDE);
    ARITHMETIC_OPERATORS.add(MOD);

    HashSet<Integer> unaryOps = new HashSet<Integer>();
    unaryOps.add(KW_NOT);
    unaryOps.add(TILDE);
    UNARY_OPERATORS = Collections.unmodifiableSet(unaryOps);
  }

  public static boolean isArithmeticOp(int tokenType) {
    return ARITHMETIC_OPERATORS.contains(tokenType);
  }

  public static ASTNode parseHQL(String query) throws ParseException {
    ParseDriver driver = new ParseDriver();
    ASTNode tree = null;
    try {
      tree = driver.parse(query, new Context(new HiveConf()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    tree = ParseUtils.findRootNonNullToken(tree);
    // printAST(tree);
    return tree;
  }

  public static ASTNode parseExpr(String expr) throws ParseException {
    ParseDriver driver = new ParseDriver();
    ASTNode tree = driver.parseExpression(expr);
    return ParseUtils.findRootNonNullToken(tree);
  }

  public static void printAST(ASTNode node) {
    try {
      printAST(getHiveTokenMapping(), node, 0, 0);
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println();
  }

  /**
   * Debug function for printing query AST to stdout
   * 
   * @param node
   * @param level
   */
  public static void printAST(Map<Integer, String> tokenMapping, ASTNode node, int level, int child) {
    if (node == null || node.isNil()) {
      return;
    }

    for (int i = 0; i < level; i++) {
      System.out.print("  ");
    }

    System.out.print(node.getText() + " [" + tokenMapping.get(node.getToken().getType()) + "]");
    System.out.print(" (l" + level + "c" + child + ")");

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
    Map<Integer, String> mapping = new HashMap<Integer, String>();

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
   * Find a node in the tree rooted at root, given the path of type of tokens
   * from the root's children to the desired node
   * 
   * @param root
   * @param path
   *          starts at the level of root's children
   * @return
   */
  public static ASTNode findNodeByPath(ASTNode root, int... path) {
    for (int i = 0; i < path.length; i++) {
      int type = path[i];
      boolean hasChildWithType = false;

      for (int j = 0; j < root.getChildCount(); j++) {
        ASTNode node = (ASTNode) root.getChild(j);
        if (node.getToken().getType() == type) {
          hasChildWithType = true;
          root = node;
          // If this is the last type in path, return this node
          if (i == path.length - 1) {
            return root;
          } else {
            // Go to next level
            break;
          }
        } else {
          // Go to next sibling.
          continue;
        }
      }

      if (!hasChildWithType) {
        // No path from this level
        break;
      }
    }

    return null;
  }

  public static ASTNode copyAST(ASTNode original) {

    ASTNode copy = new ASTNode(original); // Leverage constructor

    if (original.getChildren() != null) {
      for (Object o : original.getChildren()) {
        ASTNode childCopy = copyAST((ASTNode) o);
        childCopy.setParent(copy);
        copy.addChild(childCopy);
      }
    }
    ;
    return copy;
  }

  /**
   * Breadth first traversal of AST
   * 
   * @param root
   * @param visitor
   * @throws SemanticException
   */
  public static void bft(ASTNode root, ASTNodeVisitor visitor) throws SemanticException {
    if (root == null) {
      throw new NullPointerException("Root cannot be null");
    }

    if (visitor == null) {
      throw new NullPointerException("Visitor cannot be null");
    }
    Queue<TreeNode> queue = new LinkedList<TreeNode>();
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
    if (P_WSPACE.matcher(text).find()) {
      return true;
    }
    return false;
  }

  /**
   * Recursively reconstruct query string given a query AST
   * 
   * @param root
   * @param buf
   *          preallocated builder where the reconstructed string will be
   *          written
   */
  public static void toInfixString(ASTNode root, StringBuilder buf) {
    if (root == null) {
      return;
    }
    int rootType = root.getToken().getType();
    String rootText = root.getText();
    // Operand, print contents
    if (Identifier == rootType || Number == rootType || StringLiteral == rootType || KW_TRUE == rootType
        || KW_FALSE == rootType || KW_FORMATTED == rootType || KW_EXTENDED == rootType || KW_DEPENDENCY == rootType) {
      // StringLiterals should not be lower cased.
      if (StringLiteral == rootType) {
        buf.append(' ').append(rootText).append(' ');
      } else if (KW_TRUE == rootType) {
        buf.append(" true ");
      } else if (KW_FALSE == rootType) {
        buf.append(" false ");
      } else if (Identifier == rootType && TOK_SELEXPR == ((ASTNode) root.getParent()).getToken().getType()
          && hasSpaces(rootText)) {
        // If column alias contains spaces, enclose in back quotes
        buf.append(" as `").append(rootText).append("` ");
      } else if (Identifier == rootType && TOK_FUNCTIONSTAR == ((ASTNode) root.getParent()).getToken().getType()) {
        // count(*) or count(someTab.*): Don't append space after the identifier
        buf.append(" ").append(rootText == null ? "" : rootText.toLowerCase());
      } else {
        buf.append(" ").append(rootText == null ? "" : rootText.toLowerCase()).append(" ");
      }

    } else if (TOK_ALLCOLREF == rootType) {
      if (root.getChildCount() > 0) {
        for (int i = 0; i < root.getChildCount(); i++) {
          toInfixString((ASTNode) root.getChild(i), buf);
        }
        buf.append(".");
      }
      buf.append(" * ");
    } else if (TOK_FUNCTIONSTAR == rootType) {
      if (root.getChildCount() > 0) {
        for (int i = 0; i < root.getChildCount(); i++) {
          toInfixString((ASTNode) root.getChild(i), buf);
        }
      }
      buf.append("(*) ");
    } else if (UNARY_OPERATORS.contains(Integer.valueOf(rootType))) {
      if (KW_NOT == rootType) {
        // Check if this is actually NOT IN
        if (!(findNodeByPath(root, TOK_FUNCTION, KW_IN) != null)) {
          buf.append(" not ");
        }
      } else if (TILDE == rootType) {
        buf.append(" ~ ");
      }

      for (int i = 0; i < root.getChildCount(); i++) {
        toInfixString((ASTNode) root.getChild(i), buf);
      }

    } else if (BINARY_OPERATORS.contains(Integer.valueOf(root.getToken().getType()))) {
      buf.append("(");
      // Left operand
      toInfixString((ASTNode) root.getChild(0), buf);
      // Operand name
      if (root.getToken().getType() != DOT) {
        buf.append(' ').append(rootText.toLowerCase()).append(' ');
      } else {
        buf.append(rootText.toLowerCase());
      }
      // Right operand
      toInfixString((ASTNode) root.getChild(1), buf);
      buf.append(")");

    } else if (LSQUARE == rootType) {
      // square brackets for array and map types
      toInfixString((ASTNode) root.getChild(0), buf);
      buf.append("[");
      toInfixString((ASTNode) root.getChild(1), buf);
      buf.append("]");

    } else if (TOK_FUNCTION == root.getToken().getType()) {
      // Handle UDFs, conditional operators.
      functionString(root, buf);

    } else if (TOK_FUNCTIONDI == rootType) {
      // Distinct is a different case.
      String fname = ((ASTNode) root.getChild(0)).getText();

      buf.append(fname.toLowerCase()).append("( distinct ");

      // Arguments to distinct separated by comma
      for (int i = 1; i < root.getChildCount(); i++) {
        toInfixString((ASTNode) root.getChild(i), buf);
        if (i != root.getChildCount() - 1) {
          buf.append(", ");
        }
      }
      buf.append(")");

    } else if (TOK_TABSORTCOLNAMEDESC == rootType || TOK_TABSORTCOLNAMEASC == rootType) {
      // buf.append("(");
      for (int i = 0; i < root.getChildCount(); i++) {
        StringBuilder orderByCol = new StringBuilder();
        toInfixString((ASTNode) root.getChild(i), orderByCol);
        String colStr = orderByCol.toString().trim();
        if (colStr.startsWith("(") && colStr.endsWith(")")) {
          colStr = colStr.substring(1, colStr.length() - 1);
        }
        buf.append(colStr);
        buf.append(" ");
      }
      if (TOK_TABSORTCOLNAMEDESC == rootType) {
        buf.append(" desc ");
      } else if (TOK_TABSORTCOLNAMEASC == rootType) {
        buf.append(" asc ");
      }
      // buf.append(")");
    } else if (TOK_SELECT == rootType || TOK_ORDERBY == rootType || TOK_GROUPBY == rootType) {
      for (int i = 0; i < root.getChildCount(); i++) {
        toInfixString((ASTNode) root.getChild(i), buf);
        if (i != root.getChildCount() - 1) {
          buf.append(", ");
        }
      }

    } else if (TOK_SELECTDI == rootType) {
      buf.append(" distinct ");
      for (int i = 0; i < root.getChildCount(); i++) {
        toInfixString((ASTNode) root.getChild(i), buf);
        if (i != root.getChildCount() - 1) {
          buf.append(", ");
        }
      }

    } else if (TOK_DIR == rootType) {
      buf.append(" directory ");
      for (int i = 0; i < root.getChildCount(); i++) {
        toInfixString((ASTNode) root.getChild(i), buf);
      }

    } else if (TOK_LOCAL_DIR == rootType) {
      buf.append(" local directory ");
      for (int i = 0; i < root.getChildCount(); i++) {
        toInfixString((ASTNode) root.getChild(i), buf);
      }

    } else if (TOK_TAB == rootType) {
      buf.append(" table ");
      for (int i = 0; i < root.getChildCount(); i++) {
        toInfixString((ASTNode) root.getChild(i), buf);
      }

    } else {
      for (int i = 0; i < root.getChildCount(); i++) {
        toInfixString((ASTNode) root.getChild(i), buf);
      }
    }
  }

  // Get string representation of a function node in query AST
  private static void functionString(ASTNode root, StringBuilder buf) {
    // special handling for CASE udf
    if (findNodeByPath(root, KW_CASE) != null) {
      buf.append(" case ");
      toInfixString((ASTNode) root.getChild(1), buf);
      // each of the conditions
      ArrayList<Node> caseChildren = root.getChildren();
      int from = 2;
      int nchildren = caseChildren.size();
      int to = nchildren % 2 == 1 ? nchildren - 1 : nchildren;

      for (int i = from; i < to; i += 2) {
        buf.append(" when ");
        toInfixString((ASTNode) caseChildren.get(i), buf);
        buf.append(" then ");
        toInfixString((ASTNode) caseChildren.get(i + 1), buf);
      }

      // check if there is an ELSE node
      if (nchildren % 2 == 1) {
        buf.append(" else ");
        toInfixString((ASTNode) caseChildren.get(nchildren - 1), buf);
      }

      buf.append(" end ");

    } else if (findNodeByPath(root, KW_WHEN) != null) {
      // 2nd form of case statement

      buf.append(" case ");
      // each of the conditions
      ArrayList<Node> caseChildren = root.getChildren();
      int from = 1;
      int nchildren = caseChildren.size();
      int to = nchildren % 2 == 1 ? nchildren : nchildren - 1;

      for (int i = from; i < to; i += 2) {
        buf.append(" when ");
        toInfixString((ASTNode) caseChildren.get(i), buf);
        buf.append(" then ");
        toInfixString((ASTNode) caseChildren.get(i + 1), buf);
      }

      // check if there is an ELSE node
      if (nchildren % 2 == 0) {
        buf.append(" else ");
        toInfixString((ASTNode) caseChildren.get(nchildren - 1), buf);
      }

      buf.append(" end ");

    } else if (findNodeByPath(root, TOK_ISNULL) != null) {
      // IS NULL operator
      toInfixString((ASTNode) root.getChild(1), buf);
      buf.append(" is null ");

    } else if (findNodeByPath(root, TOK_ISNOTNULL) != null) {
      // IS NOT NULL operator
      toInfixString((ASTNode) root.getChild(1), buf);
      buf.append(" is not null ");

    } else if (((ASTNode) root.getChild(0)).getToken().getType() == Identifier
        && ((ASTNode) root.getChild(0)).getToken().getText().equalsIgnoreCase("between")) {
      // Handle between and not in between
      ASTNode tokTrue = findNodeByPath(root, KW_TRUE);
      ASTNode tokFalse = findNodeByPath(root, KW_FALSE);
      if (tokTrue != null) {
        // NOT BETWEEN
        toInfixString((ASTNode) root.getChild(2), buf);
        buf.append(" not between ");
        toInfixString((ASTNode) root.getChild(3), buf);
        buf.append(" and ");
        toInfixString((ASTNode) root.getChild(4), buf);
      } else if (tokFalse != null) {
        // BETWEEN
        toInfixString((ASTNode) root.getChild(2), buf);
        buf.append(" between ");
        toInfixString((ASTNode) root.getChild(3), buf);
        buf.append(" and ");
        toInfixString((ASTNode) root.getChild(4), buf);
      }

    } else if (findNodeByPath(root, KW_IN) != null) {
      // IN operator

      toInfixString((ASTNode) root.getChild(1), buf);

      // check if this is NOT In
      ASTNode rootParent = (ASTNode) root.getParent();
      if (rootParent != null && rootParent.getToken().getType() == KW_NOT) {
        buf.append(" not ");
      }

      buf.append(" in (");

      for (int i = 2; i < root.getChildCount(); i++) {
        toInfixString((ASTNode) root.getChild(i), buf);
        if (i < root.getChildCount() - 1) {
          buf.append(" , ");
        }
      }

      buf.append(")");

    } else {
      // Normal UDF
      String fname = ((ASTNode) root.getChild(0)).getText();
      // Function name
      buf.append(fname.toLowerCase()).append("(");
      // Arguments separated by comma
      for (int i = 1; i < root.getChildCount(); i++) {
        toInfixString((ASTNode) root.getChild(i), buf);
        if (i != root.getChildCount() - 1) {
          buf.append(", ");
        }
      }
      buf.append(")");
    }
  }

  public static void main(String[] args) throws Exception {
    ASTNode ast = parseHQL("select * from default_table ");

    printAST(getHiveTokenMapping(), ast, 0, 0);
  }

  public static String getString(ASTNode tree) {
    StringBuilder buf = new StringBuilder();
    toInfixString(tree, buf);
    return buf.toString();
  }

  public static String getColName(ASTNode node) {
    String colname = null;
    int nodeType = node.getToken().getType();
    if (nodeType == HiveParser.TOK_TABLE_OR_COL) {
      colname = ((ASTNode) node.getChild(0)).getText();
    } else {
      // node in 'alias.column' format
      ASTNode colIdent = (ASTNode) node.getChild(1);
      colname = colIdent.getText();
    }

    return colname;
  }

  public static boolean isAggregateAST(ASTNode node) {
    int exprTokenType = node.getToken().getType();
    if (exprTokenType == HiveParser.TOK_FUNCTION || exprTokenType == HiveParser.TOK_FUNCTIONDI
        || exprTokenType == HiveParser.TOK_FUNCTIONSTAR) {
      assert (node.getChildCount() != 0);
      if (node.getChild(0).getType() == HiveParser.Identifier) {
        String functionName = BaseSemanticAnalyzer.unescapeIdentifier(node.getChild(0).getText());
        if (FunctionRegistry.getGenericUDAFResolver(functionName) != null) {
          return true;
        }
      }
    }

    return false;
  }
}
