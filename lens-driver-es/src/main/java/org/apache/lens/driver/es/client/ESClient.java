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
import java.util.NoSuchElementException;

import org.apache.lens.api.query.ResultRow;
import org.apache.lens.driver.es.ESDriverConfig;
import org.apache.lens.driver.es.ESQuery;
import org.apache.lens.driver.es.exceptions.ESClientException;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.NonNull;

public abstract class ESClient {

  @NonNull
  protected final ESDriverConfig esDriverConfig;

  private ExecutionMode getExecutionModeFor(ESQuery esQuery) {
    return esQuery.getQueryType().equals(ESQuery.QueryType.AGGR)
      ?
      new DefaultExecutionMode(esQuery)
      :
      new ScrollingExecutionMode(esQuery);
  }

  protected abstract ESResultSet executeImpl(ESQuery esQuery) throws ESClientException;

  private abstract static class ExecutionMode {

    @NonNull
    protected final ESQuery esQuery;

    ExecutionMode(ESQuery query) {
      this.esQuery = query;
    }

    abstract ESResultSet executeInternal() throws ESClientException;

  }

  private class ScrollingExecutionMode extends ExecutionMode {

    @NonNull
    final JsonObject jsonQuery;

    ScrollingExecutionMode(ESQuery query) {
      super(query);
      jsonQuery = (JsonObject) new JsonParser().parse(query.getQuery());
    }

    ESQuery modify(int offset, int limit) {
      jsonQuery.addProperty(ESDriverConfig.FROM, offset);
      jsonQuery.addProperty(ESDriverConfig.SIZE, limit);
      return new ESQuery(
        esQuery.getIndex(),
        esQuery.getType(),
        jsonQuery.toString(),
        ImmutableList.copyOf(esQuery.getSchema()),
        ImmutableList.copyOf(esQuery.getColumns()),
        esQuery.getQueryType(),
        esQuery.getLimit()
      );
    }


    @Override
    ESResultSet executeInternal() throws ESClientException{
      final ESResultSet resultSet = executeImpl(esQuery);
      final int fetchSize = esDriverConfig.getTermFetchSize();
      final int limit = esQuery.getLimit();
      return new ESResultSet(
        limit,
        new Iterable<ResultRow>() {
          ESResultSet batch = resultSet;
          int processed = 0;

          final ESResultSet getNextBatch() throws ESClientException {
            final int toProcess = limit - processed;
            final int newFetchSize = limit == -1 || fetchSize < toProcess
              ?
              fetchSize
              :
              toProcess;
            batch = executeImpl(modify(processed, newFetchSize));
            return batch;
          }


          @Override
          public Iterator<ResultRow> iterator() {
            return new Iterator<ResultRow>() {
              @Override
              public boolean hasNext() {
                try {
                  return processed < limit
                    && (batch.hasNext() || getNextBatch().size > 0);
                } catch (ESClientException e) {
                  throw new RuntimeException("Encountered a runtime issue during execution", e);
                }
              }

              @Override
              public ResultRow next() {
                if (!hasNext()) {
                  throw new NoSuchElementException("Processed : " + processed + ", Limit : " + limit);
                }
                final ResultRow nextRow = batch.next();
                processed++;
                return nextRow;
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException("Cannot remove from es resultset!");
              }
            };
          }
        },
        resultSet.getMetadata()
      );
    }
  }

  private class DefaultExecutionMode extends ExecutionMode {

    DefaultExecutionMode(ESQuery query) {
      super(query);
    }

    @Override
    ESResultSet executeInternal() throws ESClientException {
      return executeImpl(esQuery);
    }

  }

  public ESClient(ESDriverConfig esDriverConfig, Configuration conf) {
    this.esDriverConfig = esDriverConfig;
  }

  public final ESResultSet execute(final ESQuery esQuery) throws ESClientException {
    return getExecutionModeFor(esQuery).executeInternal();
  }

  public abstract String explain(ESQuery esQuery) throws ESClientException;
}
