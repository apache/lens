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

package org.apache.lens.regression;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;

import javax.ws.rs.client.WebTarget;
import javax.xml.bind.JAXBException;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.query.*;
import org.apache.lens.regression.core.constants.QueryInventory;
import org.apache.lens.regression.core.helpers.*;
import org.apache.lens.regression.core.testHelper.BaseTestClass;
import org.apache.lens.regression.util.Util;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.util.LensUtil;

import org.apache.log4j.Logger;

import org.testng.Assert;
import org.testng.annotations.*;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;


public class ITStreamingTests extends BaseTestClass {

  private WebTarget servLens;
  private String sessionHandleString;

  private static Logger logger = Logger.getLogger(ITStreamingTests.class);
  String lensSiteConfPath = lens.getServerDir() + "/conf/lens-site.xml";

  @BeforeClass(alwaysRun = true)
  public void initialize() throws IOException, JAXBException, LensException {
    servLens = ServiceManagerHelper.init();
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp(Method method) throws Exception {
    logger.info("Test Name: " + method.getName());
    logger.info("Creating a new Session");
    sessionHandleString = sHelper.openSession(lens.getCurrentDB());
  }

  @AfterMethod(alwaysRun = true)
  public void closeSession() throws Exception {
    logger.info("Closing Session");
    sHelper.closeSession();
  }

  @DataProvider(name="query_provider")
  public Object[][] queryProvider(){
    Object[][] query = { {QueryInventory.JDBC_DIM_QUERY, new Integer(2)},
                         {QueryInventory.HIVE_CUBE_QUERY, new Integer(8)},
                         {QueryInventory.HIVE_DIM_QUERY, new Integer(9)},
    };
    return query;
  }


  @BeforeGroups("large_purge_interval")
  public void setLargePurgerInterval() throws Exception {
    HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.PURGE_INTERVAL, "1000000");
    Util.changeConfig(map, lensSiteConfPath);
    lens.restart();
  }

  @AfterGroups("large_purge_interval")
  public void restoreConfig() throws SftpException, JSchException, InterruptedException, LensException, IOException {
    Util.changeConfig(lensSiteConfPath);
    lens.restart();
  }


  @Test(enabled=true, groups= "large_purge_interval", dataProvider="query_provider")
  public void testQueryResultStreaming(String query, int expectedNumRows) throws Exception {

    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");

    //setting high timeout value so that query completes within timeout period
    QueryHandleWithResultSet qhr = (QueryHandleWithResultSet) qHelper.executeQueryTimeout(query, "100000").getData();

    InMemoryQueryResult inmemoryResult = (InMemoryQueryResult) qhr.getResult();
    Assert.assertNotNull(inmemoryResult);
    Assert.assertEquals(inmemoryResult.getRows().size(), expectedNumRows);

    LensQuery lq = qHelper.waitForCompletion(qhr.getQueryHandle());
    Assert.assertEquals(lq.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
    QueryResult queryRes = qHelper.getResultSet(qhr.getQueryHandle());

    Assert.assertTrue(queryRes instanceof PersistentQueryResult);

    PersistentQueryResult persistResult = (PersistentQueryResult) queryRes;
    Assert.assertNotNull(persistResult);
    Assert.assertEquals(persistResult.getNumRows().intValue(), expectedNumRows);
  }


  @Test(enabled=true, groups= "large_purge_interval")
  public void testQueryRunningAfterTimeout() throws Exception {

    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");

    LensConf lensConf = new LensConf();
    lensConf.addProperty(LensConfConstants.CANCEL_QUERY_ON_TIMEOUT, "false");

    QueryHandleWithResultSet qhr = (QueryHandleWithResultSet) qHelper.executeQueryTimeout(
        QueryInventory.getSleepQuery("5"), "10", null, sessionHandleString, lensConf).getData();
    InMemoryQueryResult inmemoryResult = (InMemoryQueryResult) qhr.getResult();
    Assert.assertNull(inmemoryResult);

    LensQuery lensQuery = qHelper.waitForCompletion(qhr.getQueryHandle());
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    PersistentQueryResult persistResult = (PersistentQueryResult)qHelper.getResultSet(qhr.getQueryHandle());
    Assert.assertNotNull(persistResult);
    Assert.assertEquals(persistResult.getNumRows().intValue(), 8);
  }


  @Test(enabled=true, groups= "large_purge_interval")
  public void resultMoreThanMaxPrefetchRows() throws Exception {

    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    sHelper.setAndValidateParam(LensConfConstants.PREFETCH_INMEMORY_RESULTSET_ROWS, "5");

    //Result of this query has 8 rows
    QueryHandleWithResultSet qhr = (QueryHandleWithResultSet) qHelper.executeQueryTimeout(QueryInventory.
        HIVE_CUBE_QUERY, "100000").getData();

    //Since query has more rows than prefetch limit, ResultSet should be PersistentQueryResult
    Assert.assertFalse(qhr.getResult() instanceof InMemoryQueryResult);

    PersistentQueryResult result = (PersistentQueryResult) qhr.getResult();
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getNumRows().intValue(), 8);

  }
}
