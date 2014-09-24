package com.inmobi.grill.driver.impala;

/*
 * #%L
 * Grill Driver for Cloudera Impala
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

import com.inmobi.grill.server.api.driver.*;
import com.inmobi.grill.server.api.events.GrillEventListener;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import com.cloudera.beeswax.api.BeeswaxException;
import com.cloudera.beeswax.api.Query;
import com.cloudera.beeswax.api.QueryHandle;
import com.cloudera.beeswax.api.QueryNotFoundException;
import com.cloudera.beeswax.api.QueryState;
import com.cloudera.impala.thrift.ImpalaService;
import com.cloudera.impala.thrift.ImpalaService.Client;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.query.QueryPrepareHandle;
import com.inmobi.grill.server.api.query.PreparedQueryContext;
import com.inmobi.grill.server.api.query.QueryContext;

public class ImpalaDriver implements GrillDriver {

  Logger logger = Logger.getLogger(ImpalaDriver.class.getName());

  private Client client;
  private List<String> storages = new ArrayList<String>();

  public ImpalaDriver() {
  }

  @Override
  public DriverQueryPlan explain(String query, Configuration conf) {
    /*QueryCost q = new QueryCost();
    q.setExecMode(ExecMode.INTERACTIVE);
    q.setScanMode(ScanMode.FULL_SCAN);
    q.setScanSize(-1);

    return q;*/
    return null;
  }

  public GrillResultSet execute(String query, Configuration conf)
      throws GrillException {
    Query q = new Query();
    q.query = query;
    QueryHandle queryHandle;
    try {
      queryHandle = client.query(q);
      QueryState qs = QueryState.INITIALIZED;

      while (true) {
        qs = client.get_state(queryHandle);
        logger.info("Query state is for query" + q + "is " + qs);
        if (qs == QueryState.FINISHED) {
          break;
        }
        if (qs == QueryState.EXCEPTION) {
          logger.error("Query aborted, unable to fetch data");
          throw new GrillException(
              "Query aborted, unable to fetch data");
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          logger.error(e.getMessage(), e);
          throw new GrillException(e.getMessage(), e);
        }
      }

    } catch (BeeswaxException e) {
      logger.error(e.getMessage(), e);
      throw new GrillException(e.getMessage(), e);

    } catch (TException e) {
      logger.error(e.getMessage(), e);
      throw new GrillException(e.getMessage(), e);

    } catch (QueryNotFoundException e) {
      logger.error(e.getMessage(), e);
      throw new GrillException(e.getMessage(), e);

    }

    ImpalaResultSet iResultSet = new ImpalaResultSet(client, queryHandle);
    return iResultSet;

  }

  @Override
  public void configure(Configuration conf) throws GrillException {
    final String HOST = "HOST";
    final String PORT = "PORT";
    TSocket sock = new TSocket(conf.get(HOST), conf.getInt(PORT, 9999));
    try {
      sock.open();
    } catch (TTransportException e) {
      logger.error(e.getMessage());
      throw new GrillException(e.getMessage(), e);
    }
    TBinaryProtocol protocol = new TBinaryProtocol(sock);
    this.client = new ImpalaService.Client(protocol);
    logger.info("Successfully connected to host" + conf.get(HOST) + ":"
        + conf.getInt(PORT, 9999));

  }

  @Override
  public void updateStatus(QueryContext context) {
    // TODO Auto-generated method stub
    return;
  }

  @Override
  public boolean cancelQuery(com.inmobi.grill.api.query.QueryHandle handle) {
    // TODO Auto-generated method stub
    return false;
  }

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws GrillException {
		// TODO Auto-generated method stub
		
	}

  /**
   * Add a listener for driver events
   *
   * @param driverEventListener
   */
  @Override
  public void registerDriverEventListener(GrillEventListener<DriverEvent> driverEventListener) {

  }

  @Override
  public void closeQuery(com.inmobi.grill.api.query.QueryHandle arg0)
      throws GrillException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void prepare(PreparedQueryContext pContext) throws GrillException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public DriverQueryPlan explainAndPrepare(PreparedQueryContext pContext)
      throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void closePreparedQuery(QueryPrepareHandle handle)
      throws GrillException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public GrillResultSet execute(QueryContext context) throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void executeAsync(QueryContext context) throws GrillException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public GrillResultSet fetchResultSet(QueryContext context)
      throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void closeResultSet(com.inmobi.grill.api.query.QueryHandle handle)
      throws GrillException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void registerForCompletionNotification(
      com.inmobi.grill.api.query.QueryHandle handle, long timeoutMillis,
      QueryCompletionListener listener) throws GrillException {
    throw new GrillException("Not implemented");    
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // TODO Auto-generated method stub
    
  }

}
