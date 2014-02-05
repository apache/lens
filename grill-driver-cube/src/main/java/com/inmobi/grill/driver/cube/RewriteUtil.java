package com.inmobi.grill.driver.cube;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.cube.parse.CubeQueryRewriter;
import org.apache.hadoop.hive.ql.cube.parse.HQLParser;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import com.inmobi.grill.api.GrillDriver;
import com.inmobi.grill.exception.GrillException;

public class RewriteUtil {

  static Pattern cubePattern = Pattern.compile(".*CUBE(.*)",
      Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);
  static Matcher matcher = null;

  static class CubeQueryInfo {
    int startPos;
    int endPos;
    String query;
    ASTNode cubeAST;
  }

  static List<RewriteUtil.CubeQueryInfo> findCubePositions(String query)
      throws SemanticException, ParseException {
    ASTNode ast = HQLParser.parseHQL(query);
    CubeGrillDriver.LOG.debug("User query AST:" + ast.dump());
    List<RewriteUtil.CubeQueryInfo> cubeQueries = new ArrayList<RewriteUtil.CubeQueryInfo>();
    RewriteUtil.findCubePositions(ast, cubeQueries, query.length());
    for (RewriteUtil.CubeQueryInfo cqi : cubeQueries) {
      cqi.query = query.substring(cqi.startPos, cqi.endPos);
    }
    return cubeQueries;
  }

  static void findCubePositions(ASTNode ast, List<RewriteUtil.CubeQueryInfo> cubeQueries,
      int queryEndPos)
          throws SemanticException {
    int child_count = ast.getChildCount();
    if (ast.getToken() != null) {
      if (ast.getToken().getType() == HiveParser.TOK_QUERY &&
          ((ASTNode) ast.getChild(0)).getToken().getType() == HiveParser.KW_CUBE) {
        RewriteUtil.CubeQueryInfo cqi = new RewriteUtil.CubeQueryInfo();
        cqi.cubeAST = ast;
        if (ast.getParent() != null) {
          ASTNode parent = (ASTNode) ast.getParent();
          cqi.startPos = ast.getCharPositionInLine();
          int ci = ast.getChildIndex();
          if (parent.getToken() == null ||
              parent.getToken().getType() == HiveParser.TOK_EXPLAIN) {
            // Not a sub query
            cqi.endPos = queryEndPos;
          } else if (parent.getChildCount() > ci + 1) {
            if (parent.getToken().getType() == HiveParser.TOK_SUBQUERY) {
              //one less for the next start and one for close parenthesis
              cqi.endPos = parent.getChild(ci + 1).getCharPositionInLine() - 2;
            } else if (parent.getToken().getType() == HiveParser.TOK_UNION) {
              //one less for the next start and less the size of string ' UNION ALL'
              cqi.endPos = parent.getChild(ci + 1).getCharPositionInLine() - 11;
            } else {
              // Not expected to reach here
              CubeGrillDriver.LOG.warn("Unknown query pattern found with AST:" + ast.dump());
              throw new SemanticException("Unknown query pattern");
            }
          } else {
            // last child of union all query
            cqi.endPos = parent.getParent().getChild(1).getCharPositionInLine() - 2;
          }
        }
        cubeQueries.add(cqi);
      }
      else {
        for (int child_pos = 0; child_pos < child_count; ++child_pos) {
          findCubePositions((ASTNode)ast.getChild(child_pos), cubeQueries, queryEndPos);
        }
      }
    } 
  }

  static CubeQueryRewriter getRewriter(GrillDriver driver) throws SemanticException {
    return new CubeQueryRewriter(driver.getConf());
  }

  /**
   * Replaces new lines with spaces;
   * '&&' with AND; '||' with OR // these two can be removed once HIVE-5326
   *  gets resolved
   * 
   * @return
   */
  static String getReplacedQuery(final String query) {
    String finalQuery = query.replaceAll("[\\n\\r]", " ")
        .replaceAll("&&", " AND ").replaceAll("\\|\\|", " OR ");
    return finalQuery;
  }

  public static Map<GrillDriver, String> rewriteQuery(final String query,
      List<GrillDriver> drivers) throws GrillException {
    try {
      String replacedQuery = getReplacedQuery(query);
      String lowerCaseQuery = replacedQuery.toLowerCase();
      Map<GrillDriver, String> driverQueries = new HashMap<GrillDriver, String>();
      if (lowerCaseQuery.startsWith("add") ||
          lowerCaseQuery.startsWith("set")) {
        for (GrillDriver driver : drivers) {
          driverQueries.put(driver, replacedQuery);
        } 
      } else {
        List<RewriteUtil.CubeQueryInfo> cubeQueries = findCubePositions(replacedQuery);
        for (GrillDriver driver : drivers) {
          CubeQueryRewriter rewriter = getRewriter(driver);
          StringBuilder builder = new StringBuilder();
          int start = 0;
          for (RewriteUtil.CubeQueryInfo cqi : cubeQueries) {
            if (start != cqi.startPos) {
              builder.append(replacedQuery.substring(start, cqi.startPos));
            }
            String hqlQuery = rewriter.rewrite(cqi.cubeAST).toHQL();
            builder.append(hqlQuery);
            start = cqi.endPos;
          }
          builder.append(replacedQuery.substring(start));
          CubeGrillDriver.LOG.info("Rewritten query for driver:" + driver + " is: " + builder.toString());
          driverQueries.put(driver, builder.toString());
        }
      }
      return driverQueries;
    } catch (Exception e) {
      throw new GrillException("Rewriting failed", e);
    }
  }

  public static boolean isCubeQuery(String query) {
    if (matcher == null) {
      matcher = cubePattern.matcher(query);
    } else {
      matcher.reset(query);
    }
    return matcher.matches();
  }

}
