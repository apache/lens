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
package org.apache.lens.server.query.collect;

import java.util.Set;

import org.apache.lens.server.api.query.QueryContext;

import lombok.NonNull;

/**
 * Makes a {@link QueryCollection} implementation thread safe by synchronizing all behaviours.
 *
 * @see QueryCollection
 *
 */
public class ThreadSafeQueryCollection implements QueryCollection {

  private final QueryCollection queries;

  public ThreadSafeQueryCollection(@NonNull QueryCollection queries) {
    this.queries = queries;
  }

  @Override
  public synchronized boolean add(QueryContext query) {
    return this.queries.add(query);
  }

  @Override
  public synchronized boolean addAll(Set<QueryContext> queries) {
    return this.queries.addAll(queries);
  }

  @Override
  public synchronized boolean remove(QueryContext query) {
    return this.queries.remove(query);
  }

  @Override
  public synchronized boolean removeAll(Set<QueryContext> queries) {
    return this.queries.removeAll(queries);
  }

  @Override
  public synchronized Set<QueryContext> getQueries() {
    return this.queries.getQueries();
  }

  @Override
  public synchronized Set<QueryContext> getQueries(final String user) {
    return this.queries.getQueries(user);
  }

  @Override
  public synchronized int getQueriesCount() {
    return this.queries.getQueriesCount();
  }

  @Override
  public synchronized Integer getQueryIndex(QueryContext query) {
    return this.queries.getQueryIndex(query);
  }
}
