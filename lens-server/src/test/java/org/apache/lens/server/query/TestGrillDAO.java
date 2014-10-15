package org.apache.lens.server.query;

/*
 * #%L
 * Grill Server
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

import org.apache.hadoop.conf.Configuration;
import org.apache.lens.api.GrillConf;
import org.apache.lens.api.GrillSessionHandle;
import org.apache.lens.api.query.GrillQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.server.GrillJerseyTest;
import org.apache.lens.server.GrillServices;
import org.apache.lens.server.api.query.FinishedGrillQuery;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.query.QueryApp;
import org.apache.lens.server.query.QueryExecutionServiceImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.ws.rs.core.Application;
import java.util.HashMap;
import java.util.List;

public class TestGrillDAO extends GrillJerseyTest {

  @Test
  public void testGrillServerDAO() throws Exception {
    QueryExecutionServiceImpl service = (QueryExecutionServiceImpl)
      GrillServices.get().getService("query");

    // Test insert query
    QueryContext queryContext = service.createContext("SELECT ID FROM testTable", "foo@localhost",
      new GrillConf(), new Configuration());
    long submissionTime = queryContext.getSubmissionTime();
    queryContext.setQueryName("daoTestQuery1");
    FinishedGrillQuery finishedGrillQuery = new FinishedGrillQuery(queryContext);
    finishedGrillQuery.setStatus(QueryStatus.Status.SUCCESSFUL.name());
    String finishedHandle = finishedGrillQuery.getHandle();
    service.lensServerDao.insertFinishedQuery(finishedGrillQuery);
    FinishedGrillQuery actual = service.lensServerDao.getQuery(finishedHandle);
    Assert.assertEquals(actual.getHandle(), finishedHandle);

    // Test find finished queries
    GrillSessionHandle session =
    service.openSession("foo@localhost", "bar", new HashMap<String, String>());

    List<QueryHandle> persistedHandles = service.lensServerDao.findFinishedQueries(null, null, null, submissionTime,
      System.currentTimeMillis());
    if (persistedHandles != null) {
      for (QueryHandle handle : persistedHandles) {
        GrillQuery query = service.getQuery(session, handle);
        if (!handle.getHandleId().toString().equals(finishedHandle)) {
          Assert.assertTrue(query.getStatus().isFinished(),
            query.getQueryHandle() + " STATUS=" + query.getStatus().getStatus());
        }
      }
    }

    System.out.println("@@ State = " + queryContext.getStatus().getStatus().name());
    List<QueryHandle> daoTestQueryHandles =
      service.lensServerDao.findFinishedQueries(finishedGrillQuery.getStatus(),
      queryContext.getSubmittedUser(), "daotestquery1", -1L, Long.MAX_VALUE);
    Assert.assertEquals(daoTestQueryHandles.size(), 1);
    Assert.assertEquals(daoTestQueryHandles.get(0).getHandleId().toString(), finishedHandle);
  }

  @Override
  protected int getTestPort() {
    return 101010;
  }

  @Override
  protected Application configure() {
    return new QueryApp();
  }
}
