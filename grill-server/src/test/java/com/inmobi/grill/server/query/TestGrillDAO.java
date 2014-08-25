package com.inmobi.grill.server.query;

import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestGrillDAO {

  @Test
  public void testDAO() throws Exception {
    Configuration conf = new Configuration();
    GrillServerDAO dao = new GrillServerDAO();
    dao.init(conf);
    dao.createFinishedQueriesTable();
    FinishedGrillQuery query = new FinishedGrillQuery();
    query.setHandle("adas");
    query.setSubmitter("adasdas");
    query.setUserQuery("asdsadasdasdsa");
    dao.insertFinishedQuery(query);
    Assert.assertEquals(query,
        dao.getQuery(query.getHandle()));
    dao.dropFinishedQueriesTable();
  }
}
