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
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryRewriter;
import org.apache.hadoop.hive.ql.cube.parse.HQLParser;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

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
    this.conf = conf;
    this.drivers = new ArrayList<GrillDriver>();
    loadDrivers();
    this.driverSelector = driverSelector;
  }

  public GrillResultSet execute(String query, Configuration conf)
      throws GrillException {
    Map<GrillDriver, String> driverQueries = 
        new HashMap<GrillDriver, String>();
    // 1. rewrite query to get summary tables and joins
    rewriteQuery(query, driverQueries);

    // 2. select driver to run the query
    GrillDriver driver = selectDriver(driverQueries);

    // 3. run query
    return driver.execute(driverQueries.get(driver), conf);
  }

  void rewriteQuery(String query, GrillDriver driver)
      throws SemanticException, ParseException {
    ASTNode ast = HQLParser.parseHQL(query);
    LOG.info("User query AST:" + ast.dump());
    CubeQueryRewriter rewriter = new CubeQueryRewriter(driver.getConf());
    rewriteCubeQueries(ast, rewriter);
    LOG.info("Final rewritten AST:" + ast.dump());
  }

  void rewriteCubeQueries(ASTNode ast, CubeQueryRewriter rewriter)
      throws SemanticException {
    int child_count = ast.getChildCount();
    if (ast.getToken() != null) {
      if (ast.getToken().getType() == HiveParser.TOK_QUERY &&
          ((ASTNode) ast.getChild(0)).getToken().getType() == HiveParser.KW_CUBE) {
        LOG.info("cube ast:" + ast.dump());
       // ast = rewriter.rewrite(ast).toAST(rewriter.getQLContext());
        LOG.info("Rewritten AST:" + ast);
      }
      else {
        for (int child_pos = 0; child_pos < child_count; ++child_pos) {
          rewriteCubeQueries((ASTNode)ast.getChild(child_pos), rewriter);
        }
      }
    } 
  }

  public boolean isCubeQuery(String query) {
    if (matcher == null) {
      matcher = cubePattern.matcher(query);
    } else {
      matcher.reset(query);
    }
    return matcher.matches();
  }

  private void rewriteQuery(String query,
      Map<GrillDriver, String> driverQueries)
          throws GrillException {
    try {
      boolean cubeQuery = isCubeQuery(query);
      for (GrillDriver driver : drivers) {
        String driverQuery;
        if (cubeQuery) {
          CubeQueryRewriter rewriter = new CubeQueryRewriter(driver.getConf());
          driverQuery = rewriter.rewrite(query).toHQL();
        } else {
          driverQuery = query;
        }
        driverQueries.put(driver, driverQuery);
      }
    } catch (Exception e) {
      throw new GrillException(e);
    }
  }

  private Map<QueryHandle, QueryExecutionContext> executionContexts =
      new HashMap<QueryHandle, CubeGrillDriver.QueryExecutionContext>();

  public QueryHandle executeAsync(String query, Configuration conf)
      throws GrillException {
    Map<GrillDriver, String> driverQueries = 
        new HashMap<GrillDriver, String>();
    rewriteQuery(query, driverQueries);
    
    GrillDriver driver = selectDriver(driverQueries);
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
      String> queries) {
    return driverSelector.select(drivers, queries);
  }

  static class MinQueryCostSelector implements DriverSelector {
    /**
     * Returns the driver that has the minimum query cost.
     */
    @Override
    public GrillDriver select(List<GrillDriver> drivers,
        final Map<GrillDriver, String> driverQueries) {
      return Collections.min(drivers, new Comparator<GrillDriver>() {
        @Override
        public int compare(GrillDriver d1, GrillDriver d2) {
          QueryPlan c1;
          QueryPlan c2;
          try {
            c1 = d1.explain(driverQueries.get(d1), null);
            c2 = d2.explain(driverQueries.get(d2), null);
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
    Map<GrillDriver, String> driverQueries = 
        new HashMap<GrillDriver, String>();
    rewriteQuery(query, driverQueries);
    GrillDriver driver = selectDriver(driverQueries);
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

}
