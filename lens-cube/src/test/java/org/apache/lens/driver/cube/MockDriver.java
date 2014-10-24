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
package org.apache.lens.driver.cube;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.lens.api.LensException;
import org.apache.lens.api.query.QueryCost;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryPrepareHandle;
import org.apache.lens.api.query.ResultRow;
import org.apache.lens.server.api.driver.*;
import org.apache.lens.server.api.driver.DriverQueryStatus.DriverQueryState;
import org.apache.lens.server.api.events.LensEventListener;
import org.apache.lens.server.api.query.PreparedQueryContext;
import org.apache.lens.server.api.query.QueryContext;

/**
 * The Class MockDriver.
 */
public class MockDriver implements LensDriver {

  /** The conf. */
  Configuration conf;

  /** The query. */
  String query;

  /** The io test val. */
  private int ioTestVal = -1;

  /**
   * Instantiates a new mock driver.
   */
  public MockDriver() {
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#configure(org.apache.hadoop.conf.Configuration)
   */
  @Override
  public void configure(Configuration conf) throws LensException {
    this.conf = conf;
    ioTestVal = conf.getInt("mock.driver.test.val", -1);
  }

  /**
   * The Class MockQueryPlan.
   */
  static class MockQueryPlan extends DriverQueryPlan {

    /** The query. */
    String query;

    /**
     * Instantiates a new mock query plan.
     *
     * @param query
     *          the query
     */
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

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#explain(java.lang.String, org.apache.hadoop.conf.Configuration)
   */
  @Override
  public DriverQueryPlan explain(String query, Configuration conf) throws LensException {
    return new MockQueryPlan(query);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#updateStatus(org.apache.lens.server.api.query.QueryContext)
   */
  @Override
  public void updateStatus(QueryContext context) throws LensException {
    context.getDriverStatus().setProgress(1.0);
    context.getDriverStatus().setStatusMessage("Done");
    context.getDriverStatus().setState(DriverQueryState.SUCCESSFUL);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#cancelQuery(org.apache.lens.api.query.QueryHandle)
   */
  @Override
  public boolean cancelQuery(QueryHandle handle) throws LensException {
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#closeQuery(org.apache.lens.api.query.QueryHandle)
   */
  @Override
  public void closeQuery(QueryHandle handle) throws LensException {
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#close()
   */
  @Override
  public void close() throws LensException {
  }

  /**
   * Add a listener for driver events.
   *
   * @param driverEventListener
   *          the driver event listener
   */
  @Override
  public void registerDriverEventListener(LensEventListener<DriverEvent> driverEventListener) {

  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#prepare(org.apache.lens.server.api.query.PreparedQueryContext)
   */
  @Override
  public void prepare(PreparedQueryContext pContext) throws LensException {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lens.server.api.driver.LensDriver#explainAndPrepare(org.apache.lens.server.api.query.PreparedQueryContext
   * )
   */
  @Override
  public DriverQueryPlan explainAndPrepare(PreparedQueryContext pContext) throws LensException {
    DriverQueryPlan p = new MockQueryPlan(pContext.getDriverQuery());
    p.setPrepareHandle(pContext.getPrepareHandle());
    return p;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#closePreparedQuery(org.apache.lens.api.query.QueryPrepareHandle)
   */
  @Override
  public void closePreparedQuery(QueryPrepareHandle handle) throws LensException {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#execute(org.apache.lens.server.api.query.QueryContext)
   */
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

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#executeAsync(org.apache.lens.server.api.query.QueryContext)
   */
  @Override
  public void executeAsync(QueryContext context) throws LensException {
    this.query = context.getDriverQuery();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#fetchResultSet(org.apache.lens.server.api.query.QueryContext)
   */
  @Override
  public LensResultSet fetchResultSet(QueryContext context) throws LensException {
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

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#closeResultSet(org.apache.lens.api.query.QueryHandle)
   */
  @Override
  public void closeResultSet(QueryHandle handle) throws LensException {
    // TODO Auto-generated method stub
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lens.server.api.driver.LensDriver#registerForCompletionNotification(org.apache.lens.api.query.QueryHandle
   * , long, org.apache.lens.server.api.driver.QueryCompletionListener)
   */
  @Override
  public void registerForCompletionNotification(QueryHandle handle, long timeoutMillis, QueryCompletionListener listener)
      throws LensException {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
   */
  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    ioTestVal = in.readInt();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
   */
  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeInt(ioTestVal);
  }

  public int getTestIOVal() {
    return ioTestVal;
  }

}
