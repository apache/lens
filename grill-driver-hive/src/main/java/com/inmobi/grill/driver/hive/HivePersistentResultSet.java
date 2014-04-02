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
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hive.service.cli.*;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.ResultColumn;
import com.inmobi.grill.server.api.driver.GrillResultSetMetadata;
import com.inmobi.grill.server.api.driver.PersistentResultSet;

public class HivePersistentResultSet extends PersistentResultSet {
  private final Path path;
  private final OperationHandle opHandle;
	private final CLIServiceClient client;
  private final QueryHandle queryHandle;
  private TableSchema metadata;

  public HivePersistentResultSet(Path resultSetPath, OperationHandle opHandle,
  		CLIServiceClient client, QueryHandle queryHandle) {
  	this.path = resultSetPath;
  	this.client = client;
  	this.opHandle = opHandle;
    this.queryHandle = queryHandle;
  }

  private TableSchema getTableSchema() throws HiveSQLException {
    if (metadata == null) {
      metadata = client.getResultSetMetadata(opHandle);
    }
    return metadata;
  }

  public QueryHandle getQueryHandle() {
    return queryHandle;
  }

  @Override
  public int size() throws GrillException {
    return -1;
  }

  @Override
  public String getOutputPath() throws GrillException {
    return path.toString();
  }

  @Override
  public GrillResultSetMetadata getMetadata() throws GrillException {
    return new GrillResultSetMetadata() {
      @Override
      public List<ResultColumn> getColumns() {
        List<ColumnDescriptor> descriptors;

        try {
          descriptors = getTableSchema().getColumnDescriptors();
        } catch (HiveSQLException e) {
          return null;
        }

        if (descriptors == null) {
          return null;
        }

        List<ResultColumn> columns = new ArrayList<ResultColumn>(descriptors.size());
        for (ColumnDescriptor colDesc : descriptors) {
          columns.add(new ResultColumn(colDesc.getName(), colDesc.getTypeName()));
        }
        return columns;
      }
    };
  }
}
