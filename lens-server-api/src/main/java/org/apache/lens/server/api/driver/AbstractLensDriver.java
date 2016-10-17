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
package org.apache.lens.server.api.driver;

import static org.apache.lens.server.api.LensConfConstants.*;
import static org.apache.lens.server.api.util.LensUtil.getImplementations;

import org.apache.lens.api.Priority;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.collect.WaitingQueriesSelectionPolicy;
import org.apache.lens.server.api.query.constraint.QueryLaunchingConstraint;
import org.apache.lens.server.api.retry.ChainedRetryPolicyDecider;
import org.apache.lens.server.api.retry.RetryPolicyDecider;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Abstract class for Lens Driver Implementations. Provides default
 * implementations and some utility methods for drivers
 */
@Slf4j
public abstract class AbstractLensDriver implements LensDriver {
  /**
   * Separator used for constructing fully qualified name and driver resource path
   */
  private static final char SEPARATOR = '/';

  /**
   * Driver's fully qualified name ( Example hive/hive1, jdbc/mysql1)
   */
  @Getter
  private String fullyQualifiedName = null;

  @Getter
  private Configuration conf;

  @Getter
  private ImmutableSet<QueryLaunchingConstraint> queryConstraints;
  @Getter
  private ImmutableSet<WaitingQueriesSelectionPolicy> waitingQuerySelectionPolicies;
  @Getter
  RetryPolicyDecider<QueryContext> retryPolicyDecider;
  @Getter
  private DriverQueryHook queryHook;

  @Override
  public void configure(Configuration conf, String driverType, String driverName) throws LensException {
    if (StringUtils.isBlank(driverType) || StringUtils.isBlank(driverName)) {
      throw new LensException("Driver Type and Name can not be null or empty");
    }
    fullyQualifiedName = driverType + SEPARATOR + driverName;
    this.conf = new DriverConfiguration(conf, driverType, getClass());
    this.conf.addResource(getClass().getSimpleName().toLowerCase() + "-default.xml");
    this.conf.addResource(getDriverResourcePath(getClass().getSimpleName().toLowerCase() + "-site.xml"));

    this.queryConstraints = getImplementations(QUERY_LAUNCHING_CONSTRAINT_FACTORIES_SFX, getConf());
    this.waitingQuerySelectionPolicies = getImplementations(WAITING_QUERIES_SELECTION_POLICY_FACTORIES_SFX, getConf());

    loadRetryPolicyDecider();
    loadQueryHook();
  }

  protected void loadQueryHook() throws LensException {
    try {
      queryHook = getConf().getClass(
        DRIVER_HOOK_CLASS_SFX, NoOpDriverQueryHook.class, DriverQueryHook.class
      ).newInstance();
      queryHook.setDriver(this);
    } catch (InstantiationException | IllegalAccessException e) {
      throw new LensException("Can't instantiate driver query hook for hivedriver with given class", e);
    }
  }

  protected void loadRetryPolicyDecider() throws LensException {
    this.retryPolicyDecider = ChainedRetryPolicyDecider.from(getConf(), RETRY_POLICY_CLASSES_SFX);
  }

  /**
   * Default implementation for fetchResultSet for all drivers. Should hold good in most cases.
   * Note : If a driver is sticking to this default implementation, it should
   * override {@link #createResultSet(QueryContext)}
   */
  @Override
  public LensResultSet fetchResultSet(QueryContext ctx) throws LensException {
    log.info("FetchResultSet: {}", ctx.getQueryHandle());
    if (!ctx.getDriverStatus().isSuccessful()) {
      throw new LensException("Can't fetch results for a " + ctx.getQueryHandleString() + " because it's status is "
        + ctx.getStatus());
    }
    ctx.registerDriverResult(createResultSet(ctx)); // registerDriverResult makes sure registration happens ony once
    return ctx.getDriverResult();
  }

  /**
   * This method should create ResultSet for the query represented by the context.
   * Specific driver should override this method to return driver specific LensResultSet whenever the
   * driver relies on default implementation of {@link #fetchResultSet(QueryContext)}
   *
   * Note: Default Implementation throw exception.
   *
   * @param ctx
   * @return
   */
  protected LensResultSet createResultSet(QueryContext ctx) throws LensException {
    throw new LensException(this.getClass().getSimpleName() + " should override method createResultSet(QueryContext)");
  }

  /**
   * Gets the path (relative to lens server's conf location) for the driver resource in the system. This is a utility
   * method that can be used by extending driver implementations to build path for their resources.
   *
   * @param resourceName
   * @return
   */
  protected String getDriverResourcePath(String resourceName) {
    return LensConfConstants.DRIVERS_BASE_DIR + SEPARATOR + getFullyQualifiedName()
      + SEPARATOR + resourceName;
  }

  @Override
  public Priority decidePriority(AbstractQueryContext queryContext) {
    return Priority.NORMAL;
  }

  @Override
  public StatusUpdateMethod getStatusUpdateMethod() {
    return StatusUpdateMethod.PULL;
  }

  @Override
  public void registerForCompletionNotification(QueryContext context, long timeoutMillis,
    QueryCompletionListener listener) {
    context.registerStatusUpdateListener(listener);
  }

  @Override
  public String toString() {
    return getFullyQualifiedName();
  }
}
