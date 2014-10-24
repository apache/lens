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
package org.apache.lens.driver.impala;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.StringTokenizer;

import org.apache.lens.api.LensException;
import org.apache.lens.api.query.ResultRow;
import org.apache.lens.server.api.driver.LensResultSetMetadata;
import org.apache.lens.server.api.driver.InMemoryResultSet;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.cloudera.beeswax.api.BeeswaxException;
import com.cloudera.beeswax.api.QueryHandle;
import com.cloudera.beeswax.api.QueryNotFoundException;
import com.cloudera.beeswax.api.Results;
import com.cloudera.impala.thrift.ImpalaService.Client;

/**
 * The Class ImpalaResultSet.
 */
public class ImpalaResultSet extends InMemoryResultSet {

  /** The logger. */
  private Logger logger = Logger.getLogger(ImpalaResultSet.class);

  /** The client. */
  private Client client;

  /** The a. */
  private Queue<List<Object>> a = new LinkedList<List<Object>>();

  /** The query handle. */
  private QueryHandle queryHandle;

  /** The has more data. */
  private boolean hasMoreData = true;

  /** The size. */
  private int size = 0;

  /**
   * Instantiates a new impala result set.
   *
   * @param client
   *          the client
   * @param queryHandle
   *          the query handle
   */
  public ImpalaResultSet(Client client, QueryHandle queryHandle) {
    this.client = client;
    this.queryHandle = queryHandle;

  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensResultSet#size()
   */
  @Override
  public int size() {
    return this.size;

  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.InMemoryResultSet#hasNext()
   */
  @Override
  public boolean hasNext() throws LensException {
    return (this.hasMoreData || this.a.size() != 0);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.InMemoryResultSet#next()
   */
  @Override
  public ResultRow next() throws LensException {

    Results resultSet = null;

    try {
      if (this.a.size() == 0) {
        if (this.hasMoreData) {

          resultSet = client.fetch(queryHandle, false, -1);
          List<String> results = resultSet.getData();
          size += results.size();
          this.a.addAll(convert(results));
          if (!resultSet.isHas_more()) {
            this.hasMoreData = false;
            client.close(queryHandle);
          }
        }
        if (a.size() == 0) {
          logger.error("No more rows");
          throw new LensException("No more rows ");
        } else {
          return new ResultRow(this.a.remove());
        }
      } else {
        return new ResultRow(this.a.remove());
      }
    } catch (QueryNotFoundException e) {
      logger.error(e.getMessage(), e);
      throw new LensException(e.getMessage(), e);
    } catch (BeeswaxException e) {
      logger.error(e.getMessage(), e);
      throw new LensException(e.getMessage(), e);
    } catch (TException e) {
      logger.error(e.getMessage(), e);
      throw new LensException(e.getMessage(), e);
    }

  }

  /**
   * converts the impala output to Breeze resultset format.
   *
   * @param inputList
   *          the input list
   * @return the list
   */
  private List<List<Object>> convert(List<String> inputList) {
    List<List<Object>> returnList = new ArrayList<List<Object>>();
    for (String eachElement : inputList) {
      List<Object> rowSet = new ArrayList<Object>();
      StringTokenizer st = new StringTokenizer(eachElement);
      while (st.hasMoreElements()) {
        rowSet.add(st.nextToken());
      }
      returnList.add(rowSet);

    }
    return returnList;
  }

  @Override
  public LensResultSetMetadata getMetadata() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setFetchSize(int size) {
    // TODO Auto-generated method stub

  }
}
