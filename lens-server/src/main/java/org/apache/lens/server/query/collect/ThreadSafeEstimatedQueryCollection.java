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

import java.util.Set;

import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.cost.QueryCost;

import lombok.NonNull;

/**
 * Makes an implementation of {@link EstimatedQueryCollection} interface thread safe by wrapping all behaviours in
 * synchronized method.
 */
public class ThreadSafeEstimatedQueryCollection implements EstimatedQueryCollection {

  private final EstimatedQueryCollection estimatedQueries;

  public ThreadSafeEstimatedQueryCollection(@NonNull final EstimatedQueryCollection estimatedQueries) {
    this.estimatedQueries = estimatedQueries;
  }

  @Override
  public synchronized Set<QueryContext> getQueries(LensDriver driver) {
    return this.estimatedQueries.getQueries(driver);
  }

  @Override
  public synchronized int getQueriesCount(LensDriver driver) {
    return this.estimatedQueries.getQueriesCount(driver);
  }

  @Override
  public synchronized QueryCost getTotalQueryCost(String user) {
    return this.estimatedQueries.getTotalQueryCost(user);
  }

  @Override
  public synchronized Set<QueryContext> getQueries() {
    return this.estimatedQueries.getQueries();
  }

  @Override
  public synchronized Set<QueryContext> getQueries(String user) {
    return this.estimatedQueries.getQueries(user);
  }

  @Override
  public synchronized int getQueriesCount() {
    return this.estimatedQueries.getQueriesCount();
  }

  @Override
  public synchronized Integer getQueryIndex(QueryContext query) {
    return this.estimatedQueries.getQueryIndex(query);
  }

  @Override
  public synchronized boolean add(QueryContext query) {
    return this.estimatedQueries.add(query);
  }

  @Override
  public synchronized boolean addAll(Set<QueryContext> queries) {
    return this.estimatedQueries.addAll(queries);
  }

  @Override
  public synchronized boolean remove(QueryContext query) {
    return this.estimatedQueries.remove(query);
  }

  @Override
  public synchronized boolean removeAll(Set<QueryContext> queries) {
    return this.estimatedQueries.removeAll(queries);
  }

  @Override
  public synchronized String toString() {
    return getClass().getSimpleName() + "(estimatedQueries=" + this.estimatedQueries + ")";
  }
}
