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
package org.apache.lens.driver.cube;

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

/**
 * The Class CubeDriver.
 */
public class CubeDriver implements LensDriver {

  /** The Constant LOG. */
  public static final Logger LOG = Logger.getLogger(CubeDriver.class);

  /** The drivers. */
  private final List<LensDriver> drivers;

  /** The driver selector. */
  private final DriverSelector driverSelector;

  /** The conf. */
  private Configuration conf;

  /** The query contexts. */
  private Map<QueryHandle, QueryContext> queryContexts = new HashMap<QueryHandle, QueryContext>();

  /** The prepared queries. */
  private Map<QueryPrepareHandle, PreparedQueryContext> preparedQueries = new HashMap<QueryPrepareHandle, PreparedQueryContext>();

  /**
   * Instantiates a new cube driver.
   *
   * @param conf
   *          the conf
   * @throws LensException
   *           the lens exception
   */
  public CubeDriver(Configuration conf) throws LensException {
    this(conf, new MinQueryCostSelector());
  }

  /**
   * Instantiates a new cube driver.
   *
   * @param conf
   *          the conf
   * @param driverSelector
   *          the driver selector
   * @throws LensException
   *           the lens exception
   */
  public CubeDriver(Configuration conf, DriverSelector driverSelector) throws LensException {
    this.conf = new HiveConf(conf, CubeDriver.class);
    this.drivers = new ArrayList<LensDriver>();
    loadDrivers();
    this.driverSelector = driverSelector;
  }

  /**
   * Load drivers.
   *
   * @throws LensException
   *           the lens exception
   */
  private void loadDrivers() throws LensException {
    String[] driverClasses = conf.getStrings(LensConfConstants.DRIVER_CLASSES);
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

  /**
   * Select driver.
   *
   * @param queries
   *          the queries
   * @param conf
   *          the conf
   * @return the lens driver
   */
  protected LensDriver selectDriver(Map<LensDriver, String> queries, Configuration conf) {
    return driverSelector.select(drivers, queries, conf);
  }

  /**
   * The Class MinQueryCostSelector.
   */
  public static class MinQueryCostSelector implements DriverSelector {

    /**
     * Returns the driver that has the minimum query cost.
     *
     * @param drivers
     *          the drivers
     * @param driverQueries
     *          the driver queries
     * @param conf
     *          the conf
     * @return the lens driver
     */
    @Override
    public LensDriver select(Collection<LensDriver> drivers, final Map<LensDriver, String> driverQueries,
        final Configuration conf) {
      return Collections.min(drivers, new Comparator<LensDriver>() {
        @Override
        public int compare(LensDriver d1, LensDriver d2) {
          DriverQueryPlan c1 = null;
          DriverQueryPlan c2 = null;
          // Handle cases where the queries can be null because the storages are not
          // supported.
          if (driverQueries.get(d1) == null) {
            return 1;
          }
          if (driverQueries.get(d2) == null) {
            return -1;
          }
          try {
            c1 = d1.explain(driverQueries.get(d1), conf);
          } catch (LensException e) {
            LOG.warn("Explain query:" + driverQueries.get(d1) + " on Driver:" + d1.getClass().getSimpleName()
                + " failed", e);
          }
          try {
            c2 = d2.explain(driverQueries.get(d2), conf);
          } catch (LensException e) {
            LOG.warn("Explain query:" + driverQueries.get(d2) + " on Driver:" + d2.getClass().getSimpleName()
                + " failed", e);
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

  /**
   * Execute.
   *
   * @param query
   *          the query
   * @param conf
   *          the conf
   * @return the lens result set
   * @throws LensException
   *           the lens exception
   */
  public LensResultSet execute(String query, Configuration conf) throws LensException {
    QueryContext ctx = createQueryContext(query, conf);
    LensResultSet result = execute(ctx);
    queryContexts.remove(ctx.getQueryHandle());
    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#execute(org.apache.lens.server.api.query.QueryContext)
   */
  @Override
  public LensResultSet execute(QueryContext ctx) throws LensException {
    rewriteAndSelect(ctx);
    return ctx.getSelectedDriver().execute(ctx);
  }

  /**
   * Rewrite and select.
   *
   * @param ctx
   *          the ctx
   * @throws LensException
   *           the lens exception
   */
  private void rewriteAndSelect(QueryContext ctx) throws LensException {
    queryContexts.put(ctx.getQueryHandle(), ctx);
    Map<LensDriver, String> driverQueries = RewriteUtil.rewriteQuery(ctx.getUserQuery(), drivers, ctx.getConf());

    // 2. select driver to run the query
    LensDriver driver = selectDriver(driverQueries, conf);

    ctx.setSelectedDriver(driver);
    ctx.setDriverQuery(driverQueries.get(driver));
  }

  /**
   * Creates the query context.
   *
   * @param query
   *          the query
   * @param conf
   *          the conf
   * @return the query context
   */
  private QueryContext createQueryContext(String query, Configuration conf) {
    return new QueryContext(query, null, conf);
  }

  /**
   * Execute async.
   *
   * @param query
   *          the query
   * @param conf
   *          the conf
   * @return the query handle
   * @throws LensException
   *           the lens exception
   */
  public QueryHandle executeAsync(String query, Configuration conf) throws LensException {
    QueryContext ctx = createQueryContext(query, conf);
    executeAsync(ctx);
    return ctx.getQueryHandle();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#executeAsync(org.apache.lens.server.api.query.QueryContext)
   */
  @Override
  public void executeAsync(QueryContext ctx) throws LensException {
    rewriteAndSelect(ctx);
    ctx.getSelectedDriver().executeAsync(ctx);
  }

  /**
   * Gets the status.
   *
   * @param handle
   *          the handle
   * @return the status
   * @throws LensException
   *           the lens exception
   */
  public QueryStatus getStatus(QueryHandle handle) throws LensException {
    updateStatus(getContext(handle));
    QueryStatus status = getContext(handle).getDriverStatus().toQueryStatus();
    if (status.getStatus().equals(QueryStatus.Status.EXECUTED)) {
      return DriverQueryStatus.createQueryStatus(QueryStatus.Status.SUCCESSFUL, getContext(handle).getDriverStatus());
    }
    return status;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#updateStatus(org.apache.lens.server.api.query.QueryContext)
   */
  public void updateStatus(QueryContext context) throws LensException {
    context.getSelectedDriver().updateStatus(context);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#fetchResultSet(org.apache.lens.server.api.query.QueryContext)
   */
  @Override
  public LensResultSet fetchResultSet(QueryContext context) throws LensException {
    return context.getSelectedDriver().fetchResultSet(context);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#configure(org.apache.hadoop.conf.Configuration)
   */
  @Override
  public void configure(Configuration conf) {
    this.conf = conf;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#cancelQuery(org.apache.lens.api.query.QueryHandle)
   */
  @Override
  public boolean cancelQuery(QueryHandle handle) throws LensException {
    return getContext(handle).getSelectedDriver().cancelQuery(handle);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#close()
   */
  @Override
  public void close() throws LensException {
    for (LensDriver driver : drivers) {
      driver.close();
    }
    drivers.clear();
    queryContexts.clear();
  }

  /**
   * Add a listener for driver events.
   *
   * @param driverEventListener
   *          the driver event listener
   */
  @Override
  public void registerDriverEventListener(LensEventListener<DriverEvent> driverEventListener) {

  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#closeQuery(org.apache.lens.api.query.QueryHandle)
   */
  @Override
  public void closeQuery(QueryHandle handle) throws LensException {
    getContext(handle).getSelectedDriver().closeQuery(handle);
    queryContexts.remove(handle);
  }

  /**
   * Gets the context.
   *
   * @param handle
   *          the handle
   * @return the context
   * @throws LensException
   *           the lens exception
   */
  private QueryContext getContext(QueryHandle handle) throws LensException {
    QueryContext ctx = queryContexts.get(handle);
    if (ctx == null) {
      throw new LensException("Query not found " + ctx);
    }
    return ctx;
  }

  public List<LensDriver> getDrivers() {
    return drivers;
  }

  /**
   * Rewrite and select for prepare.
   *
   * @param ctx
   *          the ctx
   * @throws LensException
   *           the lens exception
   */
  private void rewriteAndSelectForPrepare(PreparedQueryContext ctx) throws LensException {
    preparedQueries.put(ctx.getPrepareHandle(), ctx);
    Map<LensDriver, String> driverQueries = RewriteUtil.rewriteQuery(ctx.getUserQuery(), drivers, ctx.getConf());

    // 2. select driver to run the query
    LensDriver driver = selectDriver(driverQueries, conf);

    ctx.setSelectedDriver(driver);
    ctx.setDriverQuery(driverQueries.get(driver));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#explain(java.lang.String, org.apache.hadoop.conf.Configuration)
   */
  @Override
  public DriverQueryPlan explain(String query, Configuration conf) throws LensException {
    Map<LensDriver, String> driverQueries = RewriteUtil.rewriteQuery(query, drivers, conf);
    LensDriver driver = selectDriver(driverQueries, conf);
    return driver.explain(driverQueries.get(driver), conf);
  }

  /**
   * Execute prepare.
   *
   * @param handle
   *          the handle
   * @param conf
   *          the conf
   * @return the lens result set
   * @throws LensException
   *           the lens exception
   */
  @Deprecated
  public LensResultSet executePrepare(QueryHandle handle, Configuration conf) throws LensException {
    QueryPrepareHandle pHandle = new QueryPrepareHandle(handle.getHandleId());
    QueryContext ctx = new QueryContext(preparedQueries.get(pHandle), null, conf);
    ctx.setQueryHandle(handle);
    return execute(ctx);
  }

  /**
   * Execute prepare async.
   *
   * @param handle
   *          the handle
   * @param conf
   *          the conf
   * @throws LensException
   *           the lens exception
   */
  @Deprecated
  public void executePrepareAsync(QueryHandle handle, Configuration conf) throws LensException {
    QueryPrepareHandle pHandle = new QueryPrepareHandle(handle.getHandleId());
    QueryContext ctx = new QueryContext(preparedQueries.get(pHandle), null, conf);
    ctx.setQueryHandle(handle);
    executeAsync(ctx);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#prepare(org.apache.lens.server.api.query.PreparedQueryContext)
   */
  @Override
  public void prepare(PreparedQueryContext pContext) throws LensException {
    rewriteAndSelectForPrepare(pContext);
    pContext.getSelectedDriver().prepare(pContext);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lens.server.api.driver.LensDriver#explainAndPrepare(org.apache.lens.server.api.query.PreparedQueryContext
   * )
   */
  @Override
  public DriverQueryPlan explainAndPrepare(PreparedQueryContext pContext) throws LensException {
    LOG.info("In explainAndPrepare, preparing :" + pContext.getUserQuery());
    rewriteAndSelectForPrepare(pContext);
    return pContext.getSelectedDriver().explainAndPrepare(pContext);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#closePreparedQuery(org.apache.lens.api.query.QueryPrepareHandle)
   */
  @Override
  public void closePreparedQuery(QueryPrepareHandle handle) throws LensException {
    PreparedQueryContext ctx = preparedQueries.remove(handle);
    if (ctx != null) {
      ctx.getSelectedDriver().closePreparedQuery(handle);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#closeResultSet(org.apache.lens.api.query.QueryHandle)
   */
  @Override
  public void closeResultSet(QueryHandle handle) throws LensException {
    getContext(handle).getSelectedDriver().closeResultSet(handle);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lens.server.api.driver.LensDriver#registerForCompletionNotification(org.apache.lens.api.query.QueryHandle
   * , long, org.apache.lens.server.api.driver.QueryCompletionListener)
   */
  @Override
  public void registerForCompletionNotification(QueryHandle handle, long timeoutMillis, QueryCompletionListener listener)
      throws LensException {
    throw new LensException("Not implemented");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
   */
  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    drivers.clear();
    Map<String, LensDriver> driverMap = new HashMap<String, LensDriver>();
    synchronized (drivers) {
      drivers.clear();
      int numDrivers = in.readInt();
      for (int i = 0; i < numDrivers; i++) {
        String driverClsName = in.readUTF();
        LensDriver driver;
        try {
          Class<? extends LensDriver> driverCls = (Class<? extends LensDriver>) Class.forName(driverClsName);
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
      for (int i = 0; i < numQueries; i++) {
        QueryContext ctx = (QueryContext) in.readObject();
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

  /*
   * (non-Javadoc)
   * 
   * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
   */
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
