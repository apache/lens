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
package org.apache.lens.driver.es.client;

import java.util.Iterator;

import org.apache.lens.api.query.ResultRow;
import org.apache.lens.server.api.driver.InMemoryResultSet;
import org.apache.lens.server.api.driver.LensResultSetMetadata;

import lombok.NonNull;

/**
 * The class ESResultset for iterating over elastic search result set
 */
public class ESResultSet extends InMemoryResultSet {

  @NonNull
  final Iterator<ResultRow> resultSetIterator;
  @NonNull
  final LensResultSetMetadata resultSetMetadata;
  final Integer size;

  public ESResultSet(int size, final Iterable<ResultRow> resultSetIterable, final LensResultSetMetadata metadata) {
    this.size = size;
    this.resultSetIterator = resultSetIterable.iterator();
    this.resultSetMetadata = metadata;
  }

  @Override
  public boolean hasNext(){
    return resultSetIterator.hasNext();
  }

  @Override
  public ResultRow next() {
    return resultSetIterator.next();
  }

  @Override
  public void setFetchSize(int i) {

  }

  @Override
  public Integer size() {
    return size;
  }

  @Override
  public LensResultSetMetadata getMetadata() {
    return resultSetMetadata;
  }
}
