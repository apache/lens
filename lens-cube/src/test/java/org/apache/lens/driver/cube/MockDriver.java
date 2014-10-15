package org.apache.lens.driver.cube;

/*
 * #%L
 * Grill Cube Driver
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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.lens.api.LensException;
import org.apache.lens.api.query.QueryCost;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryPrepareHandle;
import org.apache.lens.api.query.QueryResult;
import org.apache.lens.api.query.QueryResultSetMetadata;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.api.query.ResultColumn;
import org.apache.lens.api.query.ResultRow;
import org.apache.lens.server.api.driver.*;
import org.apache.lens.server.api.driver.DriverQueryStatus.DriverQueryState;
import org.apache.lens.server.api.events.LensEventListener;
import org.apache.lens.server.api.query.PreparedQueryContext;
import org.apache.lens.server.api.query.QueryContext;


public class MockDriver implements LensDriver {

  Configuration conf;
  String query;
  private int ioTestVal = -1;

  public MockDriver() {    
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void configure(Configuration conf) throws LensException {
    this.conf = conf;
    ioTestVal = conf.getInt("mock.driver.test.val", -1);
  }

  static class MockQueryPlan extends DriverQueryPlan {
    String query;
    MockQueryPlan(String query) {
      this.query = query;
      setPrepareHandle(new QueryPrepareHandle(UUID.randomUUID()));
    }
    @Override
    public String getPlan() {
      return query;
    }

    @Override
    public QueryCost getCost() {
      return new QueryCost(0L, 0.0);
    }
  }

  @Override
  public DriverQueryPlan explain(String query, Configuration conf)
      throws LensException {
    return new MockQueryPlan(query);
  }

  @Override
  public void updateStatus(QueryContext context) throws LensException {
    context.getDriverStatus().setProgress(1.0);
    context.getDriverStatus().setStatusMessage("Done");
    context.getDriverStatus().setState(DriverQueryState.SUCCESSFUL);
  }

  @Override
  public boolean cancelQuery(QueryHandle handle) throws LensException {
    return false;
  }

  @Override
  public void closeQuery(QueryHandle handle) throws LensException {
  }

  @Override
  public void close() throws LensException {
  }

  /**
   * Add a listener for driver events
   *
   * @param driverEventListener
   */
  @Override
  public void registerDriverEventListener(LensEventListener<DriverEvent> driverEventListener) {

  }

  @Override
  public void prepare(PreparedQueryContext pContext) throws LensException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public DriverQueryPlan explainAndPrepare(PreparedQueryContext pContext)
      throws LensException {
    DriverQueryPlan p = new MockQueryPlan(pContext.getDriverQuery());
    p.setPrepareHandle(pContext.getPrepareHandle());
    return p;
  }

  @Override
  public void closePreparedQuery(QueryPrepareHandle handle)
      throws LensException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public LensResultSet execute(QueryContext context) throws LensException {
    this.query = context.getDriverQuery();
    return new PersistentResultSet() {
      
      @Override
      public int size() throws LensException {
        // TODO Auto-generated method stub
        return 0;
      }
      
      @Override
      public LensResultSetMetadata getMetadata() throws LensException {
        // TODO Auto-generated method stub
        return new LensResultSetMetadata() {

          @Override
          public List<ColumnDescriptor> getColumns() {
            // TODO Auto-generated method stub
            return null;
          }
        };
      }
      
      @Override
      public String getOutputPath() throws LensException {
        // TODO Auto-generated method stub
        return null;
      }
    };
  }

  @Override
  public void executeAsync(QueryContext context) throws LensException {
    this.query = context.getDriverQuery();
  }

  @Override
  public LensResultSet fetchResultSet(QueryContext context)
      throws LensException {
    return new InMemoryResultSet() {
      
      @Override
      public int size() throws LensException {
        // TODO Auto-generated method stub
        return 0;
      }
      
      @Override
      public LensResultSetMetadata getMetadata() throws LensException {
        return new LensResultSetMetadata() {

          @Override
          public List<ColumnDescriptor> getColumns() {
            // TODO Auto-generated method stub
            return null;
          }
        };
      }
      
      @Override
      public void setFetchSize(int size) throws LensException {
        // TODO Auto-generated method stub
        
      }
      
      @Override
      public ResultRow next() throws LensException {
        // TODO Auto-generated method stub
        return null;
      }
      
      @Override
      public boolean hasNext() throws LensException {
        // TODO Auto-generated method stub
        return false;
      }
    };
  }

  @Override
  public void closeResultSet(QueryHandle handle) throws LensException {
    // TODO Auto-generated method stub
  }

  @Override
  public void registerForCompletionNotification(QueryHandle handle,
      long timeoutMillis, QueryCompletionListener listener)
      throws LensException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    ioTestVal = in.readInt();
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeInt(ioTestVal);
  }

  public int getTestIOVal() {
    return ioTestVal;
  }

}
