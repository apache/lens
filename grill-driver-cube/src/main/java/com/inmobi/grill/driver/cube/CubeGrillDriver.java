package com.inmobi.grill.driver.cube;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryRewriter;
import org.apache.hadoop.hive.ql.cube.parse.HQLParser;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import com.inmobi.grill.api.GrillConfConstatnts;
import com.inmobi.grill.api.GrillDriver;
import com.inmobi.grill.api.GrillResultSet;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryPlan;
import com.inmobi.grill.api.QueryStatus;
import com.inmobi.grill.exception.GrillException;

public class CubeGrillDriver implements GrillDriver {
  public static final Logger LOG = Logger.getLogger(CubeGrillDriver.class);
  public static final String ENGINE_CONF_PREFIX = "grill.cube";
  public static final String ENGINE_DRIVER_CLASSES = "grill.cube.drivers";

  Pattern cubePattern = Pattern.compile(".*CUBE(.*)",
      Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);
  private final List<GrillDriver> drivers;
  private final DriverSelector driverSelector;
  private Configuration conf;
  Matcher matcher = null;

  public CubeGrillDriver(Configuration conf) throws GrillException {
    this(conf, new MinQueryCostSelector());
  }

  public CubeGrillDriver(Configuration conf, DriverSelector driverSelector)
      throws GrillException {
    this.conf = new HiveConf(conf, CubeGrillDriver.class);
    this.drivers = new ArrayList<GrillDriver>();
    loadDrivers();
    this.driverSelector = driverSelector;
  }

  public GrillResultSet execute(String query, Configuration conf)
      throws GrillException {
    Map<GrillDriver, String> driverQueries = rewriteQuery(query);

    // 2. select driver to run the query
    GrillDriver driver = selectDriver(driverQueries, conf);

    // 3. run query
    return driver.execute(driverQueries.get(driver), conf);
  }

  List<CubeQueryInfo> findCubePositions(String query)
      throws SemanticException, ParseException {
    ASTNode ast = HQLParser.parseHQL(query);
    LOG.debug("User query AST:" + ast.dump());
    List<CubeQueryInfo> cubeQueries = new ArrayList<CubeQueryInfo>();
    findCubePositions(ast, cubeQueries, query.length());
    for (CubeQueryInfo cqi : cubeQueries) {
      cqi.query = query.substring(cqi.startPos, cqi.endPos);
    }
    return cubeQueries;
  }

  private void findCubePositions(ASTNode ast, List<CubeQueryInfo> cubeQueries,
      int queryEndPos)
          throws SemanticException {
    int child_count = ast.getChildCount();
    if (ast.getToken() != null) {
      if (ast.getToken().getType() == HiveParser.TOK_QUERY &&
          ((ASTNode) ast.getChild(0)).getToken().getType() == HiveParser.KW_CUBE) {
        CubeQueryInfo cqi = new CubeQueryInfo();
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
              LOG.warn("Unknown query pattern found with AST:" + ast.dump());
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

  CubeQueryRewriter getRewriter(GrillDriver driver) throws SemanticException {
    return new CubeQueryRewriter(driver.getConf());
  }

  static class CubeQueryInfo {
    int startPos;
    int endPos;
    String query;
    ASTNode cubeAST;
  }

  public boolean isCubeQuery(String query) {
    if (matcher == null) {
      matcher = cubePattern.matcher(query);
    } else {
      matcher.reset(query);
    }
    return matcher.matches();
  }

  /**
   * Replaces new lines with spaces;
   * '&&' with AND; '||' with OR // these two can be removed once HIVE-5326
   *  gets resolved
   * 
   * @return
   */
  String getReplacedQuery(final String query) {
    String finalQuery = query.replaceAll("[\\n\\r]", " ")
        .replaceAll("&&", " AND ").replaceAll("\\|\\|", " OR ");
    return finalQuery;
  }

  Map<GrillDriver, String> rewriteQuery(final String query)
      throws GrillException {
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
        List<CubeQueryInfo> cubeQueries = findCubePositions(replacedQuery);
        for (GrillDriver driver : drivers) {
          CubeQueryRewriter rewriter = getRewriter(driver);
          StringBuilder builder = new StringBuilder();
          int start = 0;
          for (CubeQueryInfo cqi : cubeQueries) {
            if (start != cqi.startPos) {
              builder.append(replacedQuery.substring(start, cqi.startPos));
            }
            String hqlQuery = rewriter.rewrite(cqi.cubeAST).toHQL();
            builder.append(hqlQuery);
            start = cqi.endPos;
          }
          builder.append(replacedQuery.substring(start));
          LOG.info("Rewritten query for driver:" + driver + " is: " + builder.toString());
          driverQueries.put(driver, builder.toString());
        }
      }
      return driverQueries;
    } catch (Exception e) {
      throw new GrillException("Rewriting failed", e);
    }
  }

  private Map<QueryHandle, QueryExecutionContext> executionContexts =
      new HashMap<QueryHandle, CubeGrillDriver.QueryExecutionContext>();

  public QueryHandle executeAsync(String query, Configuration conf)
      throws GrillException {
    Map<GrillDriver, String> driverQueries = rewriteQuery(query);

    GrillDriver driver = selectDriver(driverQueries, conf);
    QueryHandle handle = driver.executeAsync(driverQueries.get(driver), conf);
    executionContexts.put(handle, new QueryExecutionContext(query, driver,
        driverQueries.get(driver), ExecutionStatus.STARTED));
    return handle;
  }

  public QueryStatus getStatus(QueryHandle handle) throws GrillException {
    return getContext(handle).selectedDriver.getStatus(handle);
  }

  public GrillResultSet fetchResults(QueryHandle handle) throws GrillException {
    return getContext(handle).selectedDriver.fetchResultSet(handle);
  }

  private enum ExecutionStatus {PREPARED, STARTED};

  private static class QueryExecutionContext {
    final String cubeQuery;
    final GrillDriver selectedDriver;
    final String driverQuery;
    ExecutionStatus execStatus;

    QueryExecutionContext(String cubeQuery, GrillDriver selectedDriver,
        String driverQuery, ExecutionStatus execStatus) {
      this.cubeQuery = cubeQuery;
      this.selectedDriver = selectedDriver;
      this.driverQuery = driverQuery;
      this.execStatus = execStatus;
    }
  }

  private void loadDrivers() throws GrillException {
    String[] driverClasses = conf.getStrings(ENGINE_DRIVER_CLASSES);
    if (driverClasses != null) {
      for (String driverClass : driverClasses) {
        try {
          Class<?> clazz = Class.forName(driverClass);
          GrillDriver driver = (GrillDriver) clazz.newInstance();
          driver.configure(conf);
          drivers.add(driver);
        } catch (Exception e) {
          LOG.warn("Could not load the driver:" + driverClass, e);
          throw new GrillException("Could not load driver " + driverClass, e);
        }
      }
    } else {
      throw new GrillException("No drivers specified");
    }
  }

  protected GrillDriver selectDriver(Map<GrillDriver,
      String> queries, Configuration conf) {
    return driverSelector.select(drivers, queries, conf);
  }

  static class MinQueryCostSelector implements DriverSelector {
    /**
     * Returns the driver that has the minimum query cost.
     */
    @Override
    public GrillDriver select(List<GrillDriver> drivers,
        final Map<GrillDriver, String> driverQueries, final Configuration conf) {
      return Collections.min(drivers, new Comparator<GrillDriver>() {
        @Override
        public int compare(GrillDriver d1, GrillDriver d2) {
          QueryPlan c1;
          QueryPlan c2;
          conf.setBoolean(GrillConfConstatnts.PREPARE_ON_EXPLAIN, false);
          try {
            c1 = d1.explain(driverQueries.get(d1), conf);
            c2 = d2.explain(driverQueries.get(d2), conf);
          } catch (GrillException e) {
            throw new RuntimeException("Could not compare drivers", e);
          }
          return c1.getCost().compareTo(c2.getCost());
        }
      });
    }
  }

  @Override
  public QueryPlan explain(String query, Configuration conf)
      throws GrillException {
    Map<GrillDriver, String> driverQueries = rewriteQuery(query);
    GrillDriver driver = selectDriver(driverQueries, conf);
    QueryPlan plan = driver.explain(driverQueries.get(driver), conf);
    QueryExecutionContext context = new QueryExecutionContext(query,
        driver, driverQueries.get(driver), ExecutionStatus.PREPARED) ;
    executionContexts.put(plan.getHandle(), context);
    return plan;
  }

  @Override
  public GrillResultSet executePrepare(QueryHandle handle, Configuration conf)
      throws GrillException {
    QueryExecutionContext context = executionContexts.get(handle);
    context.execStatus = ExecutionStatus.STARTED;
    return context.selectedDriver.executePrepare(handle, conf);
  }

  @Override
  public void executePrepareAsync(QueryHandle handle, Configuration conf)
      throws GrillException {
    QueryExecutionContext context = executionContexts.get(handle);
    context.execStatus = ExecutionStatus.STARTED;
    context.selectedDriver.executePrepareAsync(handle, conf);
  }

  @Override
  public GrillResultSet fetchResultSet(QueryHandle handle)
      throws GrillException {
    return getContext(handle).selectedDriver.fetchResultSet(handle);
  }

  @Override
  public void configure(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public boolean cancelQuery(QueryHandle handle) throws GrillException {
    return getContext(handle).selectedDriver.cancelQuery(handle);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void close() throws GrillException {
    drivers.clear();
    executionContexts.clear();
  }

  @Override
  public void closeQuery(QueryHandle handle) throws GrillException {
    getContext(handle).selectedDriver.closeQuery(handle);
    executionContexts.remove(handle);
  }

  private QueryExecutionContext getContext(QueryHandle handle)
      throws GrillException {
    QueryExecutionContext ctx = executionContexts.get(handle);
    if (ctx == null) {
      throw new GrillException("Query not found " + ctx); 
    }
    return ctx;
  }

  List<GrillDriver> getDrivers() {
    return drivers;
  }
}
