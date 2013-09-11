package com.inmobi.grill.driver.cube;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryRewriter;

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

  private final List<GrillDriver> drivers;
  private final DriverSelector driverSelector;
  private Configuration conf;

  public CubeGrillDriver(Configuration conf) throws GrillException {
    this(conf, new MinQueryCostSelector());
  }

  public CubeGrillDriver(Configuration conf, DriverSelector driverSelector)
      throws GrillException {
    this.conf = conf;
    loadDrivers();
    this.drivers = new ArrayList<GrillDriver>();
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
    return driver.execute(driverQueries.get(driver), null);
  }

  private void rewriteQuery(String query,
      Map<GrillDriver, String> driverQueries)
          throws GrillException {
    try {
      for (GrillDriver driver : drivers) {
        CubeQueryRewriter rewriter = new CubeQueryRewriter(driver.getConf());
        driverQueries.put(driver, rewriter.rewrite(query).toHQL());
      }
    } catch (Exception e) {
      throw new GrillException(e);
    }
  }

  private Map<QueryHandle, QueryExecutionContext> executionContexts;

  public QueryHandle executeAsync(String query, Configuration conf)
      throws GrillException {
    Map<GrillDriver, String> driverQueries = 
        new HashMap<GrillDriver, String>();
    rewriteQuery(query, driverQueries);
    
    GrillDriver driver = selectDriver(driverQueries);
    QueryHandle handle = driver.executeAsync(driverQueries.get(driver), null);
    executionContexts.put(handle, new QueryExecutionContext(query, driver,
        driverQueries.get(driver), ExecutionStatus.STARTED));
    return handle;
  }

  public QueryStatus getStatus(QueryHandle handle) throws GrillException {
    return executionContexts.get(handle).selectedDriver.getStatus(handle);
  }

  public GrillResultSet fetchResults(QueryHandle handle) throws GrillException {
    return executionContexts.get(handle).selectedDriver.fetchResultSet(handle);
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
          throw new GrillException ("Could not load driver " + driverClass, e);
        }
      }
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
    QueryPlan plan = driver.explain(driverQueries.get(driver), null);
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
    return executionContexts.get(handle).selectedDriver.fetchResultSet(handle);
  }

  @Override
  public void configure(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public boolean cancelQuery(QueryHandle handle) throws GrillException {
    return executionContexts.get(handle).selectedDriver.cancelQuery(handle);
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
    executionContexts.remove(handle);
  }
}
