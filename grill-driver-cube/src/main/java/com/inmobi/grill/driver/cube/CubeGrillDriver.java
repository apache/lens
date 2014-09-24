package com.inmobi.grill.driver.cube;

/*
 * #%L
 * Grill Cube Driver
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.inmobi.grill.server.api.driver.*;
import com.inmobi.grill.server.api.events.GrillEventListener;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryPrepareHandle;
import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.query.PreparedQueryContext;
import com.inmobi.grill.server.api.query.QueryContext;

public class CubeGrillDriver implements GrillDriver {
  public static final Logger LOG = Logger.getLogger(CubeGrillDriver.class);

  private final List<GrillDriver> drivers;
  private final DriverSelector driverSelector;
  private Configuration conf;
  private Map<QueryHandle, QueryContext> queryContexts =
      new HashMap<QueryHandle, QueryContext>();
  private Map<QueryPrepareHandle, PreparedQueryContext> preparedQueries =
      new HashMap<QueryPrepareHandle, PreparedQueryContext>();


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

  private void loadDrivers() throws GrillException {
    String[] driverClasses = conf.getStrings(
        GrillConfConstants.ENGINE_DRIVER_CLASSES);
    if (driverClasses != null) {
      for (String driverClass : driverClasses) {
        try {
          Class<?> clazz = Class.forName(driverClass);
          GrillDriver driver = (GrillDriver) clazz.newInstance();
          driver.configure(conf);
          drivers.add(driver);
          LOG.info("Cube driver loaded driver " + driverClass);
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

  public static class MinQueryCostSelector implements DriverSelector {
    /**
     * Returns the driver that has the minimum query cost.
     */
    @Override
    public GrillDriver select(Collection<GrillDriver> drivers,
        final Map<GrillDriver, String> driverQueries, final Configuration conf) {
      return Collections.min(drivers, new Comparator<GrillDriver>() {
        @Override
        public int compare(GrillDriver d1, GrillDriver d2) {
          DriverQueryPlan c1 = null;
          DriverQueryPlan c2 = null;
          conf.setBoolean(GrillConfConstants.PREPARE_ON_EXPLAIN, false);
          //Handle cases where the queries can be null because the storages are not
          //supported.
          if(driverQueries.get(d1) == null) {
            return 1;
          }
          if(driverQueries.get(d2) == null) {
            return -1;
          }
          try {
            c1 = d1.explain(driverQueries.get(d1), conf);
          } catch (GrillException e) {
            LOG.warn("Explain query:" + driverQueries.get(d1) +
                " on Driver:" + d1.getClass().getSimpleName() + " failed", e);
          }
          try {
            c2 = d2.explain(driverQueries.get(d2), conf);
          } catch (GrillException e) {
            LOG.warn("Explain query:" + driverQueries.get(d2) +
                " on Driver:" + d2.getClass().getSimpleName() + " failed", e);
          }
          if (c1 == null && c2 == null) {
            return 0;
          } else if (c1 == null && c2 != null) {
            return 1;
          } else if (c1 != null && c2 == null) {
            return -1;
          }
          return c1.getCost().compareTo(c2.getCost());
        }
      });
    }
  }

  public GrillResultSet execute(String query, Configuration conf)
      throws GrillException {
    QueryContext ctx =  createQueryContext(query, conf);
    GrillResultSet result = execute(ctx);
    queryContexts.remove(ctx.getQueryHandle());
    return result;
  }

  @Override
  public GrillResultSet execute(QueryContext ctx) throws GrillException {
    rewriteAndSelect(ctx);
    return ctx.getSelectedDriver().execute(ctx);
  }

  private void rewriteAndSelect(QueryContext ctx) throws GrillException {
    queryContexts.put(ctx.getQueryHandle(), ctx);
    Map<GrillDriver, String> driverQueries = RewriteUtil.rewriteQuery(
        ctx.getUserQuery(), drivers, ctx.getConf());

    // 2. select driver to run the query
    GrillDriver driver = selectDriver(driverQueries, conf);

    ctx.setSelectedDriver(driver);
    ctx.setDriverQuery(driverQueries.get(driver));
  }

  private QueryContext createQueryContext(String query, Configuration conf) {
    return new QueryContext(query, null, conf);
  }

  public QueryHandle executeAsync(String query, Configuration conf)
      throws GrillException {
    QueryContext ctx = createQueryContext(query, conf);
    executeAsync(ctx);
    return ctx.getQueryHandle();
  }

  @Override
  public void executeAsync(QueryContext ctx) throws GrillException {
    rewriteAndSelect(ctx);
    ctx.getSelectedDriver().executeAsync(ctx);
  }

  public QueryStatus getStatus(QueryHandle handle) throws GrillException {
    updateStatus(getContext(handle));
    QueryStatus status = getContext(handle).getDriverStatus().toQueryStatus();
    if (status.getStatus().equals(QueryStatus.Status.EXECUTED)) {
      return DriverQueryStatus.createQueryStatus(QueryStatus.Status.SUCCESSFUL,
          getContext(handle).getDriverStatus());
    }
    return status;
  }

  public void updateStatus(QueryContext context) throws GrillException {
    context.getSelectedDriver().updateStatus(context);
  }

  @Override
  public GrillResultSet fetchResultSet(QueryContext context)
      throws GrillException {
    return context.getSelectedDriver().fetchResultSet(context);
  }

  @Override
  public void configure(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public boolean cancelQuery(QueryHandle handle) throws GrillException {
    return getContext(handle).getSelectedDriver().cancelQuery(handle);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void close() throws GrillException {
    for (GrillDriver driver : drivers) {
      driver.close();
    }
    drivers.clear();
    queryContexts.clear();
  }

  /**
   * Add a listener for driver events
   *
   * @param driverEventListener
   */
  @Override
  public void registerDriverEventListener(GrillEventListener<DriverEvent> driverEventListener) {

  }

  @Override
  public void closeQuery(QueryHandle handle) throws GrillException {
    getContext(handle).getSelectedDriver().closeQuery(handle);
    queryContexts.remove(handle);
  }

  private QueryContext getContext(QueryHandle handle)
      throws GrillException {
    QueryContext ctx = queryContexts.get(handle);
    if (ctx == null) {
      throw new GrillException("Query not found " + ctx); 
    }
    return ctx;
  }

  public List<GrillDriver> getDrivers() {
    return drivers;
  }

  private void rewriteAndSelectForPrepare(PreparedQueryContext ctx)
      throws GrillException {
    preparedQueries.put(ctx.getPrepareHandle(), ctx);
    Map<GrillDriver, String> driverQueries = RewriteUtil.rewriteQuery(
        ctx.getUserQuery(), drivers, ctx.getConf());

    // 2. select driver to run the query
    GrillDriver driver = selectDriver(driverQueries, conf);
    
    ctx.setSelectedDriver(driver);
    ctx.setDriverQuery(driverQueries.get(driver));
  }

  @Override
  public DriverQueryPlan explain(String query, Configuration conf)
      throws GrillException {
    if (conf.getBoolean(GrillConfConstants.PREPARE_ON_EXPLAIN,
        GrillConfConstants.DEFAULT_PREPARE_ON_EXPLAIN)) {
      PreparedQueryContext ctx = new PreparedQueryContext(query, null, conf);
      return explainAndPrepare(ctx);
    }
    Map<GrillDriver, String> driverQueries = RewriteUtil.rewriteQuery(
        query, drivers, conf);
    GrillDriver driver = selectDriver(driverQueries, conf);
    return driver.explain(driverQueries.get(driver), conf);
  }

  @Deprecated
  public GrillResultSet executePrepare(QueryHandle handle, Configuration conf)
      throws GrillException {
    QueryPrepareHandle pHandle = new QueryPrepareHandle(handle.getHandleId());
    QueryContext ctx = new QueryContext(preparedQueries.get(pHandle), null, conf);
    ctx.setQueryHandle(handle);
    return execute(ctx);
  }

  @Deprecated
  public void executePrepareAsync(QueryHandle handle, Configuration conf)
      throws GrillException {
    QueryPrepareHandle pHandle = new QueryPrepareHandle(handle.getHandleId());
    QueryContext ctx = new QueryContext(preparedQueries.get(pHandle), null, conf);
    ctx.setQueryHandle(handle);
    executeAsync(ctx);
  }

  @Override
  public void prepare(PreparedQueryContext pContext) throws GrillException {
    rewriteAndSelectForPrepare(pContext);
    pContext.getSelectedDriver().prepare(pContext);
  }

  @Override
  public DriverQueryPlan explainAndPrepare(PreparedQueryContext pContext)
      throws GrillException {
    LOG.info("In explainAndPrepare, preparing :" + pContext.getUserQuery());
    rewriteAndSelectForPrepare(pContext);
    return pContext.getSelectedDriver().explainAndPrepare(pContext);
  }

  @Override
  public void closePreparedQuery(QueryPrepareHandle handle)
      throws GrillException {
    PreparedQueryContext ctx = preparedQueries.remove(handle);
    if (ctx != null) {
      ctx.getSelectedDriver().closePreparedQuery(handle);
    }
  }

  @Override
  public void closeResultSet(QueryHandle handle) throws GrillException {
    getContext(handle).getSelectedDriver().closeResultSet(handle);
  }

  @Override
  public void registerForCompletionNotification(QueryHandle handle,
      long timeoutMillis, QueryCompletionListener listener)
      throws GrillException {
    throw new GrillException("Not implemented");
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    drivers.clear();
    Map<String, GrillDriver> driverMap = new HashMap<String, GrillDriver>();
    synchronized (drivers) {
      drivers.clear();
      int numDrivers = in.readInt();
      for (int i =0; i < numDrivers; i++) {
        String driverClsName = in.readUTF();
        GrillDriver driver;
        try {
          Class<? extends GrillDriver> driverCls = 
              (Class<? extends GrillDriver>)Class.forName(driverClsName);
          driver = (GrillDriver) driverCls.newInstance();
          driver.configure(conf);
        } catch (Exception e) {
          LOG.error("Could not instantiate driver:" + driverClsName);
          throw new IOException(e);
        }
        driver.readExternal(in);
        drivers.add(driver);
        driverMap.put(driverClsName, driver);
      }
    }

    synchronized (queryContexts) {
      int numQueries = in.readInt();
      for (int i =0; i < numQueries; i++) {
        QueryContext ctx = (QueryContext)in.readObject();
        queryContexts.put(ctx.getQueryHandle(), ctx);
        boolean driverAvailable = in.readBoolean();
        if (driverAvailable) {
          String clsName = in.readUTF();
          ctx.setSelectedDriver(driverMap.get(clsName));
        }
      }
      LOG.info("Recovered " + queryContexts.size() + " queries");
    }
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // persist all drivers
    synchronized (drivers) {
      out.writeInt(drivers.size());
      for (GrillDriver driver : drivers) {
        out.writeUTF(driver.getClass().getName());
        driver.writeExternal(out);
      }
    }
    // persist allQueries
    synchronized (queryContexts) {
      out.writeInt(queryContexts.size());
      for (QueryContext ctx : queryContexts.values()) {
        out.writeObject(ctx);
        boolean isDriverAvailable = (ctx.getSelectedDriver() != null);
        out.writeBoolean(isDriverAvailable);
        if (isDriverAvailable) {
          out.writeUTF(ctx.getSelectedDriver().getClass().getName());
        }
      }
    }
    LOG.info("Persisted " + queryContexts.size() + " queries");

  }
}
