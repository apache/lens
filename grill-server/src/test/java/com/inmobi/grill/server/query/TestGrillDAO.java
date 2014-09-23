package com.inmobi.grill.server.query;

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

import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.query.GrillQuery;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.server.GrillJerseyTest;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.query.FinishedGrillQuery;
import com.inmobi.grill.server.api.query.QueryContext;
import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.Application;
import java.util.HashMap;
import java.util.List;

public class TestGrillDAO extends GrillJerseyTest {
  GrillServerDAO dao;

  @BeforeClass
  public void setup() {
    Configuration conf = new Configuration();
    dao = new GrillServerDAO();
    dao.init(conf);
  }

  @Test
  public void testGrillServerDAO() throws Exception {
    QueryExecutionServiceImpl service = (QueryExecutionServiceImpl)
      GrillServices.get().getService("query");

    // Test insert query
    QueryContext queryContext = service.createContext("SELECT ID FROM testTable", "foo@localhost",
      new GrillConf(), new Configuration());
    queryContext.setQueryName("daoTestQuery1");
    FinishedGrillQuery finishedGrillQuery = new FinishedGrillQuery(queryContext);
    finishedGrillQuery.setStatus(QueryStatus.Status.SUCCESSFUL.name());
    String finishedHandle = finishedGrillQuery.getHandle();
    dao.insertFinishedQuery(finishedGrillQuery);
    FinishedGrillQuery actual = dao.getQuery(finishedHandle);
    Assert.assertEquals(actual.getHandle(), finishedHandle);

    // Test find finished queries
    GrillSessionHandle session =
    service.openSession("foo@localhost", "bar", new HashMap<String, String>());

    List<QueryHandle> persistedHandles = dao.findFinishedQueries(null, null, null);
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
      dao.findFinishedQueries(finishedGrillQuery.getStatus(),
      queryContext.getSubmittedUser(), "daotestquery1");
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
