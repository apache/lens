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
package org.apache.lens.server;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.query.QueryExecutionServiceImpl;

import org.apache.hive.service.cli.CLIService;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MockQueryExecutionServiceImpl extends QueryExecutionServiceImpl {

  private static final long INITIAL_SLEEP_MILLIS = 5000; //5 secs
  private static final long DECREASE_BY_MILLIS = 1000; //1 sec
  private long sleepInterval = INITIAL_SLEEP_MILLIS;
  public static final String ENABLE_SLEEP_FOR_GET_QUERY_OP = "test.MockQueryExecutionServiceImpl.getQuery.sleep";

  /**
   * Instantiates a new query execution service impl.
   *
   * @param cliService the cli service
   * @throws LensException the lens exception
   */
  public MockQueryExecutionServiceImpl(CLIService cliService) throws LensException {
    super(cliService);
    log.info("Using MockQueryExecutionServiceImpl as QueryExecutionService implementation");
  }

  @Override
  public LensQuery getQuery(LensSessionHandle sessionHandle, QueryHandle queryHandle) throws LensException {

    if (getSession(sessionHandle).getSessionConf().get(ENABLE_SLEEP_FOR_GET_QUERY_OP) != null) {
      //Introduce wait time for requests on this session. The wait time decreases with each new request to
      //this method and finally becomes zero and is then reinitialized to INITIAL_SLEEP_MILLIS.
      // This is used to simulate READ_TIMEOUT on client.
      // Note : the logic is not synchronized as such scenario/need is not expected from test case
      if (sleepInterval > 0) {
        try {
          log.info("MockQueryExecutionServiceImpl.getQuery Sleeping for {} millis", sleepInterval);
          Thread.sleep(sleepInterval);
        } catch (InterruptedException e) {
          //Ignore
        }
      }
      sleepInterval = sleepInterval - DECREASE_BY_MILLIS;
      if (sleepInterval < 0) {
        sleepInterval = INITIAL_SLEEP_MILLIS;
      }
    }

    return super.getQuery(sessionHandle, queryHandle);
  }
}
