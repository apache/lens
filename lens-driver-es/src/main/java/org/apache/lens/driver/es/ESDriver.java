/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.driver.es;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryPrepareHandle;
import org.apache.lens.cube.metadata.CubeMetastoreClient;
import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.driver.es.client.ESClient;
import org.apache.lens.driver.es.client.ESResultSet;
import org.apache.lens.driver.es.client.jest.JestClientImpl;
import org.apache.lens.driver.es.translator.ESVisitor;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.*;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.events.LensEventListener;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.PreparedQueryContext;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.collect.WaitingQueriesSelectionPolicy;
import org.apache.lens.server.api.query.constraint.QueryLaunchingConstraint;
import org.apache.lens.server.api.query.cost.FactPartitionBasedQueryCost;
import org.apache.lens.server.api.query.cost.QueryCost;

import org.apache.commons.lang.Validate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.antlr.runtime.CommonToken;
import org.antlr.runtime.tree.Tree;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;

/**
 * Driver for elastic search
 */
@Slf4j
public class ESDriver extends AbstractLensDriver {

  private static final AtomicInteger THID = new AtomicInteger();
  private static final double STREAMING_PARTITION_COST = 0;
  private static final QueryCost ES_DRIVER_COST = new FactPartitionBasedQueryCost(STREAMING_PARTITION_COST);

  private Configuration conf;
  private ESClient esClient;
  private ExecutorService asyncQueryPool;
  private ESDriverConfig config;

  /**
   * States
   */
  private final Map<String, ESQuery> rewrittenQueriesCache = Maps.newConcurrentMap();
  private final Map<QueryHandle, Future<LensResultSet>> resultSetMap = Maps.newConcurrentMap();
  private final Map<QueryHandle, QueryCompletionListener> handleListenerMap = Maps.newConcurrentMap();

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public QueryCost estimate(AbstractQueryContext qctx) {
    return ES_DRIVER_COST;
  }

  @Override
  public DriverQueryPlan explain(final AbstractQueryContext context) throws LensException {
    final ESQuery esQuery = rewrite(context);
    final String jsonExplanation = esClient.explain(esQuery);
    if (jsonExplanation == null) {
      throw new LensException("Explanation failed, empty json was returned");
    }
    return new DriverQueryPlan() {
      @Override
      public String getPlan() {
        return jsonExplanation;
      }

      @Override
      public QueryCost getCost() {
        return ES_DRIVER_COST;
      }
    };
  }

  @Override
  public void prepare(PreparedQueryContext pContext) throws LensException {
    rewrite(pContext);
  }

  @Override
  public DriverQueryPlan explainAndPrepare(PreparedQueryContext pContext) throws LensException {
    prepare(pContext);
    return explain(pContext);
  }

  @Override
  public void closePreparedQuery(QueryPrepareHandle handle) {
    /**
     * Elastic search does not have a concept of prepared query.
     */
  }

  @Override
  public LensResultSet execute(QueryContext context) throws LensException {
    final ESQuery esQuery = rewrite(context);
    final QueryHandle queryHandle = context.getQueryHandle();
    final ESResultSet resultSet = esClient.execute(esQuery);
    notifyComplIfRegistered(queryHandle);
    return resultSet;
  }

  @Override
  public void executeAsync(final QueryContext context) {
    final Future<LensResultSet> futureResult
      = asyncQueryPool.submit(new ESQueryExecuteCallable(context, SessionState.get()));
    resultSetMap.put(context.getQueryHandle(), futureResult);
  }

  @Override
  public void registerForCompletionNotification(QueryHandle handle, long timeoutMillis,
                                                QueryCompletionListener listener) {
    handleListenerMap.put(handle, listener);
  }

  @Override
  public void updateStatus(QueryContext context) {
    final QueryHandle queryHandle = context.getQueryHandle();
    final Future<LensResultSet> lensResultSetFuture = resultSetMap.get(queryHandle);
    if (lensResultSetFuture == null) {
      context.getDriverStatus().setState(DriverQueryStatus.DriverQueryState.CLOSED);
      context.getDriverStatus().setStatusMessage(queryHandle + " closed");
      context.getDriverStatus().setResultSetAvailable(false);
    } else if (lensResultSetFuture.isDone()) {
      context.getDriverStatus().setState(DriverQueryStatus.DriverQueryState.SUCCESSFUL);
      context.getDriverStatus().setStatusMessage(queryHandle + " successful");
      context.getDriverStatus().setResultSetAvailable(true);
    } else if (lensResultSetFuture.isCancelled()) {
      context.getDriverStatus().setState(DriverQueryStatus.DriverQueryState.CANCELED);
      context.getDriverStatus().setStatusMessage(queryHandle + " cancelled");
      context.getDriverStatus().setResultSetAvailable(false);
    }
  }

  @Override
  public LensResultSet fetchResultSet(QueryContext context) throws LensException {
    try {
      /**
       * removing the result set as soon as the fetch is done
       */
      return resultSetMap.remove(context.getQueryHandle()).get();
    } catch (NullPointerException e) {
      throw new LensException("The results for the query "
        + context.getQueryHandleString()
        + "has already been fetched");
    } catch (InterruptedException | ExecutionException e) {
      throw new LensException("Error fetching result set!", e);
    }
  }

  @Override
  public void closeResultSet(QueryHandle handle) throws LensException {
    try {
      resultSetMap.remove(handle);
    } catch (NullPointerException e) {
      throw new LensException("The query does not exist or was already purged", e);
    }
  }

  @Override
  public boolean cancelQuery(QueryHandle handle) throws LensException {
    try {
      boolean cancelled = resultSetMap.get(handle).cancel(true);
      if (cancelled) {
        notifyQueryCancellation(handle);
      }
      return cancelled;
    } catch (NullPointerException e) {
      throw new LensException("The query does not exist or was already purged", e);
    }
  }

  @Override
  public void closeQuery(QueryHandle handle) throws LensException {
    cancelQuery(handle);
    closeResultSet(handle);
    handleListenerMap.remove(handle);
  }

  @Override
  public void close() throws LensException {
    for(QueryHandle handle : resultSetMap.keySet()) {
      try {
        closeQuery(handle);
      } catch (LensException e) {
        log.error("Error while closing query {}", handle.getHandleIdString(), e);
      }
    }
  }

  @Override
  public void registerDriverEventListener(LensEventListener<DriverEvent> driverEventListener) {

  }

  @Override
  public ImmutableSet<QueryLaunchingConstraint> getQueryConstraints() {
    return ImmutableSet.copyOf(Sets.<QueryLaunchingConstraint>newHashSet());
  }

  @Override
  public ImmutableSet<WaitingQueriesSelectionPolicy> getWaitingQuerySelectionPolicies() {
    return ImmutableSet.copyOf(Sets.<WaitingQueriesSelectionPolicy>newHashSet());
  }

  private void notifyComplIfRegistered(QueryHandle queryHandle) {
    try {
      handleListenerMap.get(queryHandle).onCompletion(queryHandle);
    } catch (NullPointerException e) {
      log.debug("There are no subscriptions for notification. Skipping for {}", queryHandle.getHandleIdString(), e);
    }
  }

  private void notifyQueryCancellation(QueryHandle handle) {
    try {
      handleListenerMap.get(handle).onError(handle, handle + " cancelled");
    } catch (NullPointerException e) {
      log.debug("There are no subscriptions for notification. Skipping for {}", handle.getHandleIdString(), e);
    }
  }

  private ESQuery rewrite(AbstractQueryContext context) throws LensException {
    final String key = keyFor(context);
    if (rewrittenQueriesCache.containsKey(key)) {
      return rewrittenQueriesCache.get(key);
    } else {
      final ASTNode rootQueryNode = HQLParser.parseHQL(context.getDriverQuery(this), new HiveConf());
      setIndexAndTypeIfNotPresent(context, rootQueryNode);
      final ESQuery esQuery = ESVisitor.rewrite(config, rootQueryNode);
      rewrittenQueriesCache.put(key, esQuery);
      return esQuery;
    }
  }

  private void setIndexAndTypeIfNotPresent(AbstractQueryContext context, ASTNode rootQueryNode) throws LensException {
    final ASTNode dbSchemaTable = HQLParser.findNodeByPath(
      rootQueryNode,
      HiveParser.TOK_FROM,
      HiveParser.TOK_TABREF,
      HiveParser.TOK_TABNAME);
    try {
      Validate.notNull(dbSchemaTable);
      if (dbSchemaTable.getChildren().size() == 2) {
        /**
         * Index and type is already set here
         */
        return;
      }
      /**
       * Get the table name, check metastore and set index and actual table name
       */
      final Tree firstChild = dbSchemaTable.getChild(0);
      final String lensTable = firstChild.getText();
      final Table tbl = CubeMetastoreClient.getInstance(context.getHiveConf()).getHiveTable(lensTable);
      final String index = tbl.getProperty(LensConfConstants.ES_INDEX_NAME);
      final String type = tbl.getProperty(LensConfConstants.ES_TYPE_NAME);
      Validate.notNull(index, LensConfConstants.ES_INDEX_NAME + " property missing in table definition");
      Validate.notNull(type, LensConfConstants.ES_TYPE_NAME + " property missing in table definition");
      ((ASTNode) firstChild).getToken().setText(type);
      final ASTNode indexIdentifier = new ASTNode(new CommonToken(HiveParser.Identifier, index));
      indexIdentifier.setParent(dbSchemaTable);
      dbSchemaTable.insertChild(0, indexIdentifier);
    } catch (HiveException e) {
      throw new LensException("Error occured when trying to communicate with metastore");
    }
  }

  private String keyFor(AbstractQueryContext context) {
    return String.valueOf(context.getFinalDriverQuery(this)!=null) + ":" + context.getDriverQuery(this);
  }

  ESClient getESClient() {
    return esClient;
  }

  @Override
  public void configure(Configuration conf, String driverType, String driverName) throws LensException {
    super.configure(conf, driverType, driverName);
    this.conf = new Configuration(conf);
    this.conf.addResource("esdriver-default.xml");
    this.conf.addResource(getDriverResourcePath("esdriver-site.xml"));
    config = new ESDriverConfig(this.conf);
    Class klass;
    try {
      klass = Class.forName(this.conf.get(ESDriverConfig.CLIENT_CLASS_KEY));
      if (klass != null) {
        log.debug("Picked up class {}", klass);
        if (ESClient.class.isAssignableFrom(klass)) {
          final Constructor constructor = klass.getConstructor(ESDriverConfig.class, Configuration.class);
          esClient = (ESClient) constructor.newInstance(config, this.conf);
          log.debug("Successfully instantiated es client of type {}", klass);
        }
      } else {
        log.debug("Client class not provided, falling back to the default Jest client");
        esClient = new JestClientImpl(config, conf);
      }
    } catch (ClassNotFoundException
      | NoSuchMethodException
      | InstantiationException
      | IllegalAccessException
      | InvocationTargetException e) {
      log.error("ES driver {} cannot start!", getFullyQualifiedName(), e);
      throw new LensException("Cannot start es driver", e);
    }
    log.info("ES Driver {} configured", getFullyQualifiedName());
    asyncQueryPool = Executors.newCachedThreadPool(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable runnable) {
        Thread th = new Thread(runnable);
        th.setName("lens-driver-es-" + THID.incrementAndGet());
        return th;
      }
    });
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    /**
     * This flow could be abstracted out at the driver level
     */
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    /**
     * This flow could be abstracted out at the driver level
     */
  }

  protected class ESQueryExecuteCallable implements Callable<LensResultSet> {

    private final QueryContext queryContext;
    private final SessionState sessionState;

    public ESQueryExecuteCallable(QueryContext queryContext, SessionState sessionState) {
      this.queryContext = queryContext;
      this.sessionState = sessionState;
    }

    @Override
    public LensResultSet call() throws Exception {
      SessionState.setCurrentSessionState(sessionState);
      return execute(queryContext);
    }
  }

}
