package com.inmobi.grill.driver.cube;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryContext;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryRewriter;
import org.apache.hadoop.hive.ql.cube.parse.HQLParser;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import static org.mockito.Matchers.any;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockObjectFactory;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.IObjectFactory;
import org.testng.annotations.ObjectFactory;

import com.inmobi.grill.api.GrillDriver;
import com.inmobi.grill.driver.cube.CubeGrillDriver.CubeQueryInfo;
import com.inmobi.grill.exception.GrillException;

@PrepareForTest(CubeGrillDriver.class )
@PowerMockIgnore("org.apache.log4j.*")
public class TestRewriting {

  /**
   * We need a special {@link IObjectFactory}.
   * 
   * @return {@link PowerMockObjectFactory}.
   */
  @ObjectFactory
  public IObjectFactory getObjectFactory() {
    return new org.powermock.modules.testng.PowerMockObjectFactory();
  }

  private CubeQueryRewriter getMockedRewriter()
      throws SemanticException, ParseException {
    CubeQueryRewriter mockwriter = Mockito.mock(CubeQueryRewriter.class);
    Mockito.when(mockwriter.rewrite(any(String.class))).thenAnswer(
        new Answer<CubeQueryContext>() {
          @Override
          public CubeQueryContext answer(InvocationOnMock invocation)
              throws Throwable {
            Object[] args = invocation.getArguments();
            return getMockedCubeContext((String)args[0]);
          }
        });
    Mockito.when(mockwriter.rewrite(any(ASTNode.class))).thenAnswer(
        new Answer<CubeQueryContext>() {
          @Override
          public CubeQueryContext answer(InvocationOnMock invocation)
              throws Throwable {
            Object[] args = invocation.getArguments();
            return getMockedCubeContext((ASTNode)args[0]);
          }
        });
    return mockwriter;
  }

  private CubeQueryContext getMockedCubeContext(String query)
      throws SemanticException, ParseException {
    CubeQueryContext context = Mockito.mock(CubeQueryContext.class);
    Mockito.when(context.toHQL()).thenReturn(query.substring(4));
    Mockito.when(context.toAST(any(Context.class))).thenReturn(
        HQLParser.parseHQL(query.substring(4)));
    return context;
  }

  private CubeQueryContext getMockedCubeContext(ASTNode ast)
      throws SemanticException, ParseException {
    CubeQueryContext context = Mockito.mock(CubeQueryContext.class);
    if (ast.getToken().getType() == HiveParser.TOK_QUERY) {
      if (((ASTNode) ast.getChild(0)).getToken().getType() == HiveParser.KW_CUBE) {
        // remove cube child from AST
        for (int i = 0; i < ast.getChildCount() - 1; i++) {
          ast.setChild(i, ast.getChild(i + 1));
        }
        ast.deleteChild(ast.getChildCount() - 1);
      }
    }
    StringBuilder builder = new StringBuilder();
    HQLParser.toInfixString(ast, builder);
    Mockito.when(context.toHQL()).thenReturn(builder.toString());
    Mockito.when(context.toAST(any(Context.class))).thenReturn(ast);
    return context;
  }

  @Test
  public void testCubeQuery()
      throws ParseException, SemanticException, GrillException {
    List<GrillDriver> drivers = new ArrayList<GrillDriver>();
    MockDriver driver = new MockDriver();
    driver.configure(new Configuration());
    drivers.add(driver);

    CubeQueryRewriter mockWriter = getMockedRewriter();
//    PowerMockito.spy(CubeGrillDriver.class);
    PowerMockito.stub(PowerMockito.method(CubeGrillDriver.class, "getRewriter")).toReturn(mockWriter);
    String q1 = "select name from table";
    Assert.assertFalse(CubeGrillDriver.isCubeQuery(q1));
    List<CubeQueryInfo> cubeQueries = CubeGrillDriver.findCubePositions(q1);
    Assert.assertEquals(cubeQueries.size(), 0);
    CubeGrillDriver.rewriteQuery(q1, drivers);

    String q2 = "cube select name from table";
    Assert.assertTrue(CubeGrillDriver.isCubeQuery(q2));
    cubeQueries = CubeGrillDriver.findCubePositions(q2);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    CubeGrillDriver.rewriteQuery(q2, drivers);

    q2 = "explain cube select name from table";
    Assert.assertTrue(CubeGrillDriver.isCubeQuery(q2));
    cubeQueries = CubeGrillDriver.findCubePositions(q2);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    CubeGrillDriver.rewriteQuery(q2, drivers);

    q2 = "select * from (cube select name from table) a";
    Assert.assertTrue(CubeGrillDriver.isCubeQuery(q2));
    cubeQueries = CubeGrillDriver.findCubePositions(q2);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    CubeGrillDriver.rewriteQuery(q2, drivers);

    q2 = "select * from (cube select name from table where" +
        " (name = 'ABC'||name = 'XYZ')&&(key=100)) a";
    Assert.assertTrue(CubeGrillDriver.isCubeQuery(q2));
    cubeQueries = CubeGrillDriver.findCubePositions(CubeGrillDriver.getReplacedQuery(q2));
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from" +
        " table where (name = 'ABC' OR name = 'XYZ') AND (key=100)");
    CubeGrillDriver.rewriteQuery(q2, drivers);

    q2 = "select * from (cube select name from table) a join (cube select" +
        " name2 from table2) b";
    Assert.assertTrue(CubeGrillDriver.isCubeQuery(q2));
    cubeQueries = CubeGrillDriver.findCubePositions(q2);
    Assert.assertEquals(cubeQueries.size(), 2);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    Assert.assertEquals(cubeQueries.get(1).query, "cube select name2 from table2");
    CubeGrillDriver.rewriteQuery(q2, drivers);

    q2 = "select * from (cube select name from table) a full outer join" +
        " (cube select name2 from table2) b on a.name=b.name2";
    Assert.assertTrue(CubeGrillDriver.isCubeQuery(q2));
    cubeQueries = CubeGrillDriver.findCubePositions(q2);
    Assert.assertEquals(cubeQueries.size(), 2);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    Assert.assertEquals(cubeQueries.get(1).query, "cube select name2 from table2");
    CubeGrillDriver.rewriteQuery(q2, drivers);

    q2 = "select * from (cube select name from table) a join (select name2 from table2) b";
    Assert.assertTrue(CubeGrillDriver.isCubeQuery(q2));
    cubeQueries = CubeGrillDriver.findCubePositions(q2);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    CubeGrillDriver.rewriteQuery(q2, drivers);

    q2 = "select * from (cube select name from table union all cube select name2 from table2) u";
    Assert.assertTrue(CubeGrillDriver.isCubeQuery(q2));
    cubeQueries = CubeGrillDriver.findCubePositions(q2);
    Assert.assertEquals(cubeQueries.size(), 2);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    Assert.assertEquals(cubeQueries.get(1).query, "cube select name2 from table2");
    CubeGrillDriver.rewriteQuery(q2, drivers);

    q2 = "select u.* from (select name from table union all cube select name2 from table2) u";
    Assert.assertTrue(CubeGrillDriver.isCubeQuery(q2));
    cubeQueries = CubeGrillDriver.findCubePositions(q2);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name2 from table2");
    CubeGrillDriver.rewriteQuery(q2, drivers);

    q2 = "select * from (cube select name from table union all cube select" +
         " name2 from table2) u group by u.name";
    Assert.assertTrue(CubeGrillDriver.isCubeQuery(q2));
    cubeQueries = CubeGrillDriver.findCubePositions(q2);
    Assert.assertEquals(cubeQueries.size(), 2);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    Assert.assertEquals(cubeQueries.get(1).query, "cube select name2 from table2");
    CubeGrillDriver.rewriteQuery(q2, drivers);

  }
}
