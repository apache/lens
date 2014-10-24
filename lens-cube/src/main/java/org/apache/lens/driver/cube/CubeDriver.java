package org.apache.lens.driver.cube;

/*
 * #%L
 * Lens Cube
 * %%
 * Copyright (C) 2014 Apache Software Foundation
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


import org.apache.lens.api.LensException;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryPrepareHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.*;
import org.apache.lens.server.api.events.LensEventListener;
import org.apache.lens.server.api.query.PreparedQueryContext;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;


public class CubeDriver implements LensDriver {
  public static final Logger LOG = Logger.getLogger(CubeDriver.class);

  private final List<LensDriver> drivers;
  private final DriverSelector driverSelector;
  private Configuration conf;
  private Map<QueryHandle, QueryContext> queryContexts =
      new HashMap<QueryHandle, QueryContext>();
  private Map<QueryPrepareHandle, PreparedQueryContext> preparedQueries =
      new HashMap<QueryPrepareHandle, PreparedQueryContext>();


  public CubeDriver(Configuration conf) throws LensException {
    this(conf, new MinQueryCostSelector());
  }

  public CubeDriver(Configuration conf, DriverSelector driverSelector)
      throws LensException {
    this.conf = new HiveConf(conf, CubeDriver.class);
    this.drivers = new ArrayList<LensDriver>();
    loadDrivers();
    this.driverSelector = driverSelector;
  }

  private void loadDrivers() throws LensException {
    String[] driverClasses = conf.getStrings(
        LensConfConstants.DRIVER_CLASSES);
    if (driverClasses != null) {
      for (String driverClass : driverClasses) {
        try {
          Class<?> clazz = Class.forName(driverClass);
          LensDriver driver = (LensDriver) clazz.newInstance();
          driver.configure(conf);
          drivers.add(driver);
          LOG.info("Cube driver loaded driver " + driverClass);
        } catch (Exception e) {
          LOG.warn("Could not load the driver:" + driverClass, e);
          throw new LensException("Could not load driver " + driverClass, e);
        }
      }
    } else {
      throw new LensException("No drivers specified");
    }
  }

  protected LensDriver selectDriver(Map<LensDriver,
      String> queries, Configuration conf) {
    return driverSelector.select(drivers, queries, conf);
  }

  public static class MinQueryCostSelector implements DriverSelector {
    /**
     * Returns the driver that has the minimum query cost.
     */
    @Override
    public LensDriver select(Collection<LensDriver> drivers,
        final Map<LensDriver, String> driverQueries, final Configuration conf) {
      return Collections.min(drivers, new Comparator<LensDriver>() {
        @Override
        public int compare(LensDriver d1, LensDriver d2) {
          DriverQueryPlan c1 = null;
          DriverQueryPlan c2 = null;
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
          } catch (LensException e) {
            LOG.warn("Explain query:" + driverQueries.get(d1) +
                " on Driver:" + d1.getClass().getSimpleName() + " failed", e);
          }
          try {
            c2 = d2.explain(driverQueries.get(d2), conf);
          } catch (LensException e) {
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

  public LensResultSet execute(String query, Configuration conf)
      throws LensException {
    QueryContext ctx =  createQueryContext(query, conf);
    LensResultSet result = execute(ctx);
    queryContexts.remove(ctx.getQueryHandle());
    return result;
  }

  @Override
  public LensResultSet execute(QueryContext ctx) throws LensException {
    rewriteAndSelect(ctx);
    return ctx.getSelectedDriver().execute(ctx);
  }

  private void rewriteAndSelect(QueryContext ctx) throws LensException {
    queryContexts.put(ctx.getQueryHandle(), ctx);
    Map<LensDriver, String> driverQueries = RewriteUtil.rewriteQuery(
        ctx.getUserQuery(), drivers, ctx.getConf());

    // 2. select driver to run the query
    LensDriver driver = selectDriver(driverQueries, conf);

    ctx.setSelectedDriver(driver);
    ctx.setDriverQuery(driverQueries.get(driver));
  }

  private QueryContext createQueryContext(String query, Configuration conf) {
    return new QueryContext(query, null, conf);
  }

  public QueryHandle executeAsync(String query, Configuration conf)
      throws LensException {
    QueryContext ctx = createQueryContext(query, conf);
    executeAsync(ctx);
    return ctx.getQueryHandle();
  }

  @Override
  public void executeAsync(QueryContext ctx) throws LensException {
    rewriteAndSelect(ctx);
    ctx.getSelectedDriver().executeAsync(ctx);
  }

  public QueryStatus getStatus(QueryHandle handle) throws LensException {
    updateStatus(getContext(handle));
    QueryStatus status = getContext(handle).getDriverStatus().toQueryStatus();
    if (status.getStatus().equals(QueryStatus.Status.EXECUTED)) {
      return DriverQueryStatus.createQueryStatus(QueryStatus.Status.SUCCESSFUL,
          getContext(handle).getDriverStatus());
    }
    return status;
  }

  public void updateStatus(QueryContext context) throws LensException {
    context.getSelectedDriver().updateStatus(context);
  }

  @Override
  public LensResultSet fetchResultSet(QueryContext context)
      throws LensException {
    return context.getSelectedDriver().fetchResultSet(context);
  }

  @Override
  public void configure(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public boolean cancelQuery(QueryHandle handle) throws LensException {
    return getContext(handle).getSelectedDriver().cancelQuery(handle);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void close() throws LensException {
    for (LensDriver driver : drivers) {
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
  public void registerDriverEventListener(LensEventListener<DriverEvent> driverEventListener) {

  }

  @Override
  public void closeQuery(QueryHandle handle) throws LensException {
    getContext(handle).getSelectedDriver().closeQuery(handle);
    queryContexts.remove(handle);
  }

  private QueryContext getContext(QueryHandle handle)
      throws LensException {
    QueryContext ctx = queryContexts.get(handle);
    if (ctx == null) {
      throw new LensException("Query not found " + ctx);
    }
    return ctx;
  }

  public List<LensDriver> getDrivers() {
    return drivers;
  }

  private void rewriteAndSelectForPrepare(PreparedQueryContext ctx)
      throws LensException {
    preparedQueries.put(ctx.getPrepareHandle(), ctx);
    Map<LensDriver, String> driverQueries = RewriteUtil.rewriteQuery(
        ctx.getUserQuery(), drivers, ctx.getConf());

    // 2. select driver to run the query
    LensDriver driver = selectDriver(driverQueries, conf);

    ctx.setSelectedDriver(driver);
    ctx.setDriverQuery(driverQueries.get(driver));
  }

  @Override
  public DriverQueryPlan explain(String query, Configuration conf)
      throws LensException {
    Map<LensDriver, String> driverQueries = RewriteUtil.rewriteQuery(
        query, drivers, conf);
    LensDriver driver = selectDriver(driverQueries, conf);
    return driver.explain(driverQueries.get(driver), conf);
  }

  @Deprecated
  public LensResultSet executePrepare(QueryHandle handle, Configuration conf)
      throws LensException {
    QueryPrepareHandle pHandle = new QueryPrepareHandle(handle.getHandleId());
    QueryContext ctx = new QueryContext(preparedQueries.get(pHandle), null, conf);
    ctx.setQueryHandle(handle);
    return execute(ctx);
  }

  @Deprecated
  public void executePrepareAsync(QueryHandle handle, Configuration conf)
      throws LensException {
    QueryPrepareHandle pHandle = new QueryPrepareHandle(handle.getHandleId());
    QueryContext ctx = new QueryContext(preparedQueries.get(pHandle), null, conf);
    ctx.setQueryHandle(handle);
    executeAsync(ctx);
  }

  @Override
  public void prepare(PreparedQueryContext pContext) throws LensException {
    rewriteAndSelectForPrepare(pContext);
    pContext.getSelectedDriver().prepare(pContext);
  }

  @Override
  public DriverQueryPlan explainAndPrepare(PreparedQueryContext pContext)
      throws LensException {
    LOG.info("In explainAndPrepare, preparing :" + pContext.getUserQuery());
    rewriteAndSelectForPrepare(pContext);
    return pContext.getSelectedDriver().explainAndPrepare(pContext);
  }

  @Override
  public void closePreparedQuery(QueryPrepareHandle handle)
      throws LensException {
    PreparedQueryContext ctx = preparedQueries.remove(handle);
    if (ctx != null) {
      ctx.getSelectedDriver().closePreparedQuery(handle);
    }
  }

  @Override
  public void closeResultSet(QueryHandle handle) throws LensException {
    getContext(handle).getSelectedDriver().closeResultSet(handle);
  }

  @Override
  public void registerForCompletionNotification(QueryHandle handle,
      long timeoutMillis, QueryCompletionListener listener)
          throws LensException {
    throw new LensException("Not implemented");
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    drivers.clear();
    Map<String, LensDriver> driverMap = new HashMap<String, LensDriver>();
    synchronized (drivers) {
      drivers.clear();
      int numDrivers = in.readInt();
      for (int i =0; i < numDrivers; i++) {
        String driverClsName = in.readUTF();
        LensDriver driver;
        try {
          Class<? extends LensDriver> driverCls =
              (Class<? extends LensDriver>)Class.forName(driverClsName);
          driver = (LensDriver) driverCls.newInstance();
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
      for (LensDriver driver : drivers) {
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
