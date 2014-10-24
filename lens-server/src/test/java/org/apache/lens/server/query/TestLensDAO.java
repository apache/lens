package org.apache.lens.server.query;

import org.apache.hadoop.conf.Configuration;
import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.query.FinishedLensQuery;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.query.QueryApp;
import org.apache.lens.server.query.QueryExecutionServiceImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.ws.rs.core.Application;
import java.util.HashMap;
import java.util.List;

/**
 * The Class TestLensDAO.
 */
public class TestLensDAO extends LensJerseyTest {

  /**
   * Test lens server dao.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testLensServerDAO() throws Exception {
    QueryExecutionServiceImpl service = (QueryExecutionServiceImpl) LensServices.get().getService("query");

    // Test insert query
    QueryContext queryContext = service.createContext("SELECT ID FROM testTable", "foo@localhost", new LensConf(),
        new Configuration());
    long submissionTime = queryContext.getSubmissionTime();
    queryContext.setQueryName("daoTestQuery1");
    FinishedLensQuery finishedLensQuery = new FinishedLensQuery(queryContext);
    finishedLensQuery.setStatus(QueryStatus.Status.SUCCESSFUL.name());
    String finishedHandle = finishedLensQuery.getHandle();
    service.lensServerDao.insertFinishedQuery(finishedLensQuery);
    FinishedLensQuery actual = service.lensServerDao.getQuery(finishedHandle);
    Assert.assertEquals(actual.getHandle(), finishedHandle);

    // Test find finished queries
    LensSessionHandle session = service.openSession("foo@localhost", "bar", new HashMap<String, String>());

    List<QueryHandle> persistedHandles = service.lensServerDao.findFinishedQueries(null, null, null, submissionTime,
        System.currentTimeMillis());
    if (persistedHandles != null) {
      for (QueryHandle handle : persistedHandles) {
        LensQuery query = service.getQuery(session, handle);
        if (!handle.getHandleId().toString().equals(finishedHandle)) {
          Assert.assertTrue(query.getStatus().isFinished(), query.getQueryHandle() + " STATUS="
              + query.getStatus().getStatus());
        }
      }
    }

    System.out.println("@@ State = " + queryContext.getStatus().getStatus().name());
    List<QueryHandle> daoTestQueryHandles = service.lensServerDao.findFinishedQueries(finishedLensQuery.getStatus(),
        queryContext.getSubmittedUser(), "daotestquery1", -1L, Long.MAX_VALUE);
    Assert.assertEquals(daoTestQueryHandles.size(), 1);
    Assert.assertEquals(daoTestQueryHandles.get(0).getHandleId().toString(), finishedHandle);
  }

  @Override
  protected int getTestPort() {
    return 101010;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.glassfish.jersey.test.JerseyTest#configure()
   */
  @Override
  protected Application configure() {
    return new QueryApp();
  }
}
