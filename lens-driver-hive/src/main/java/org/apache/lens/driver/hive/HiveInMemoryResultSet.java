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
package org.apache.lens.driver.hive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.lens.api.query.ResultRow;
import org.apache.lens.server.api.driver.InMemoryResultSet;
import org.apache.lens.server.api.driver.LensResultSetMetadata;
import org.apache.lens.server.api.error.LensException;

import org.apache.hive.service.cli.*;

import lombok.extern.slf4j.Slf4j;

/**
 * The Class HiveInMemoryResultSet.
 */
@Slf4j
public class HiveInMemoryResultSet extends InMemoryResultSet {

  /** The client. */
  private final CLIServiceClient client;

  /** The op handle. */
  private final OperationHandle opHandle;

  /** The metadata. */
  private TableSchema metadata;

  /** The row set. */
  private RowSet rowSet;

  /** The fetch size. */
  private int fetchSize = 100;

  /** The fetched rows itr. */
  private Iterator<Object[]> fetchedRowsItr;

  /** The no more results. */
  private boolean noMoreResults;

  /** The close after fecth. */
  private boolean closeAfterFecth;

  /** The num columns. */
  int numColumns;
  private FetchOrientation orientation;

  /**
   * Instantiates a new hive in memory result set.
   *
   * @param hiveHandle      the hive handle
   * @param client          the client
   * @param closeAfterFecth the close after fecth
   * @throws HiveSQLException the hive sql exception
   */
  public HiveInMemoryResultSet(OperationHandle hiveHandle, CLIServiceClient client, boolean closeAfterFecth)
    throws HiveSQLException {
    this.client = client;
    this.opHandle = hiveHandle;
    this.closeAfterFecth = closeAfterFecth;
    this.metadata = client.getResultSetMetadata(opHandle);
    this.numColumns = metadata.getColumnDescriptors().size();
    this.orientation = FetchOrientation.FETCH_FIRST;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.driver.LensResultSet#size()
   */
  @Override
  public Integer size() throws LensException {
    return null;
  }

  @Override
  public LensResultSetMetadata getMetadata() throws LensException {
    // Removed Anonymous inner class and changed it to concrete class
    // for serialization to JSON
    HiveResultSetMetadata hrsMeta = new HiveResultSetMetadata();
    hrsMeta.setColumns(metadata.getColumnDescriptors());
    return hrsMeta;
  }

  /*
     * (non-Javadoc)
     *
     * @see org.apache.lens.server.api.driver.InMemoryResultSet#hasNext()
     */
  @Override
  public boolean hasNext() throws LensException {
    if (fetchedRowsItr == null || !fetchedRowsItr.hasNext()) {
      try {
        rowSet = client.fetchResults(opHandle, orientation, fetchSize, FetchType.QUERY_OUTPUT);
        orientation = FetchOrientation.FETCH_NEXT;
        noMoreResults = rowSet.numRows() == 0;
        if (noMoreResults) {
          if (closeAfterFecth) {
            log.info("No more results closing the query");
            client.closeOperation(opHandle);
          }
          return false;
        }
        fetchedRowsItr = rowSet.iterator();
      } catch (Exception e) {
        throw new LensException(e);
      }
    }
    return fetchedRowsItr.hasNext();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.driver.InMemoryResultSet#next()
   */
  @Override
  public ResultRow next() throws LensException {
    List<Object> results = new ArrayList<Object>(numColumns);
    Object[] row = fetchedRowsItr.next();
    results.addAll(Arrays.asList(row));

    return new ResultRow(results);
  }

  @Override
  public void setFetchSize(int size) throws LensException {
    assert size >= 0;
    fetchSize = size == 0 ? Integer.MAX_VALUE : size;
  }
}
