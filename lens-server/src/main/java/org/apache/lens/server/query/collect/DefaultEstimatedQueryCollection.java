/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lens.server.query.collect;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.cost.FactPartitionBasedQueryCost;
import org.apache.lens.server.api.query.cost.QueryCost;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.map.MultiValueMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * Implementation which creates multiple in memory views of queries existing in lens system and owns responsibility of
 * keeping all the views consistent with each other.
 *
 * @see EstimatedQueryCollection
 *
 */
@Slf4j
@ToString
public class DefaultEstimatedQueryCollection implements EstimatedQueryCollection {

  private final QueryCollection queries;
  private final MultiValueMap queriesByDriver = MultiValueMap.decorate(new HashMap(), LinkedHashSet.class);

  public DefaultEstimatedQueryCollection(@NonNull final QueryCollection queries) {
    this.queries = queries;
  }

  @Override
  public Set<QueryContext> getQueries(final LensDriver driver) {
    final Collection<QueryContext> driverQueries = getQueriesCollectionForDriver(driver);
    return Sets.newLinkedHashSet(driverQueries);
  }

  @Override
  public int getQueriesCount(final LensDriver driver) {
    return getQueriesCollectionForDriver(driver).size();
  }

  @Override
  public QueryCost getTotalQueryCost(final String user) {

    return getTotalQueryCost(this.queries.getQueries(user));
  }

  /**
   *
   * @param query
   * @return
   * @throws IllegalStateException if selected driver or selected driver query cost is not set for the query
   */
  @Override
  public boolean add(QueryContext query) {
    checkState(query);
    this.queriesByDriver.put(query.getSelectedDriver(), query);
    return this.queries.add(query);
  }

  /**
   *
   * @param queries
   * @throws IllegalStateException if selected driver or selected driver query cost is not set for any of the queries
   */
  @Override
  public boolean addAll(Set<QueryContext> queries) {

    boolean modified = false;
    for (QueryContext query : queries) {
      modified |= add(query);
    }
    return modified;
  }

  /**
   *
   * @param query
   * @return
   */
  @Override
  public boolean remove(QueryContext query) {
    this.queriesByDriver.remove(query.getSelectedDriver(), query);
    return this.queries.remove(query);
  }

  @Override
  public boolean removeAll(Set<QueryContext> queries) {

    boolean modified = false;
    for (QueryContext query : queries) {
      modified |= remove(query);
    }
    return modified;
  }

  @Override
  public Set<QueryContext> getQueries() {
    return this.queries.getQueries();
  }

  @Override
  public Set<QueryContext> getQueries(String user) {
    return this.queries.getQueries(user);
  }

  @Override
  public int getQueriesCount() {
    return this.queries.getQueriesCount();
  }

  @Override
  public Integer getQueryIndex(QueryContext query) {
    return this.queries.getQueryIndex(query);
  }

  @VisibleForTesting
  void checkState(final QueryContext query) {
    Preconditions.checkState(query.getSelectedDriver() != null);
    Preconditions.checkState(query.getSelectedDriverQueryCost() != null);
  }

  private Collection<QueryContext> getQueriesCollectionForDriver(final LensDriver driver) {

    final Collection<QueryContext> driverQueries = queriesByDriver.getCollection(driver);
    return driverQueries != null ? driverQueries : CollectionUtils.EMPTY_COLLECTION;
  }

  private QueryCost getTotalQueryCost(final Collection<QueryContext> queries) {

    if (queries.isEmpty()) {
      return new FactPartitionBasedQueryCost(0);
    }

    QueryContext query0 = Iterables.get(queries, 0);
    QueryCost totalQueryCost = query0.getSelectedDriverQueryCost();

    for (QueryContext query : queries) {
      QueryCost queryCost = query.getSelectedDriverQueryCost();
      totalQueryCost = totalQueryCost.add(queryCost);
    }
    log.debug("Total Query Cost:{}", totalQueryCost);
    return totalQueryCost;
  }

  @Override
  public synchronized String toString() {
    return getClass().getSimpleName() + "(Queries=" + this.queries + ")";
  }
}
