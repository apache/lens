package com.inmobi.grill.driver.hive;

/*
 * #%L
 * Grill Hive Driver
 * %%
 * Copyright (C) 2014 Inmobi
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.query.ResultRow;
import com.inmobi.grill.server.api.driver.GrillResultSetMetadata;
import com.inmobi.grill.server.api.driver.InMemoryResultSet;

public class HiveInMemoryResultSet extends InMemoryResultSet {
  private final CLIServiceClient client;
  private final OperationHandle opHandle;
  private TableSchema metadata;
  private RowSet rowSet;
  private int fetchSize = 100;
  private Iterator<Object[]> fetchedRowsItr;
  private boolean noMoreResults;
  private boolean closeAfterFecth;
  int numColumns;

  public HiveInMemoryResultSet(OperationHandle hiveHandle,
      CLIServiceClient client, boolean closeAfterFecth) throws HiveSQLException {
    this.client = client;
    this.opHandle = hiveHandle;
    this.closeAfterFecth = closeAfterFecth;
    this.metadata = client.getResultSetMetadata(opHandle);
    this.numColumns = metadata.getColumnDescriptors().size();
  }

  @Override
  public int size() throws GrillException {
    return -1;
  }

  @Override
  public GrillResultSetMetadata getMetadata() throws GrillException {
    return new GrillResultSetMetadata() {

      @Override
      public List<ColumnDescriptor> getColumns() {
        return metadata.getColumnDescriptors();
      }
    };
  }

  @Override
  public boolean hasNext() throws GrillException {
    if (fetchedRowsItr == null || !fetchedRowsItr.hasNext()) {
      try {
        rowSet = client.fetchResults(opHandle, FetchOrientation.FETCH_NEXT, fetchSize);
        noMoreResults = rowSet.numRows() == 0;
        if (noMoreResults) {
          if (closeAfterFecth) {
            HiveDriver.LOG.info("No more results closing the query");
            client.closeOperation(opHandle);
          }
          return false;
        }
        fetchedRowsItr = rowSet.iterator();
      } catch (Exception e) {
        throw new GrillException(e);
      }
    }
    return fetchedRowsItr.hasNext();
  }

  @Override
  public ResultRow next() throws GrillException {
    List<Object> results = new ArrayList<Object>(numColumns);
    Object[] row = fetchedRowsItr.next();
    results.addAll(Arrays.asList(row));

    return new ResultRow(results);
  }

  @Override
  public void setFetchSize(int size) throws GrillException {
    assert size >= 0;
    fetchSize = size == 0 ? Integer.MAX_VALUE : size;
  }
}
