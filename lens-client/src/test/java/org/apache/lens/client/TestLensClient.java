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
package org.apache.lens.client;

import static org.testng.Assert.*;

import java.io.File;
import java.net.URI;
import java.util.*;

import javax.ws.rs.core.UriBuilder;
import javax.xml.datatype.DatatypeFactory;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.metastore.*;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.client.exceptions.LensClientIOException;
import org.apache.lens.client.resultset.ResultSet;
import org.apache.lens.server.LensAllApplicationJerseyTest;
import org.apache.lens.server.api.LensConfConstants;

import org.testng.Assert;
import org.testng.annotations.*;

import lombok.extern.slf4j.Slf4j;

@Test(groups = "unit-test")
@Slf4j
public class TestLensClient extends LensAllApplicationJerseyTest {
  private static final String TEST_DB = TestLensClient.class.getSimpleName();

  @Override
  protected int getTestPort() {
    return 10056;
  }

  @Override
  protected URI getBaseUri() {
    return UriBuilder.fromUri("http://localhost/").port(getTestPort()).path("/lensapi").build();
  }

  private LensClient client;

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();

    client = new LensClient();
    client.createDatabase(TEST_DB, true);
    assertTrue(client.setDatabase(TEST_DB));

    log.debug("Creating cube sample-cube");
    APIResult result = client.createCube("target/test-classes/sample-cube.xml");
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    log.debug("Creating storage local");
    result = client.createStorage("target/test-classes/local-storage.xml");
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    log.debug("Creating dimension test_dim");
    result = client.createDimension("target/test-classes/test-dimension.xml");
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    log.debug("Creating dimension test_detail");
    result = client.createDimension("target/test-classes/test-detail.xml");
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    log.debug("Creating dimension table dim_table for dimension test_dim");
    result = client.createDimensionTable("target/test-classes/dim_table.xml");
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    log.debug("adding partition to dim_table");
    XPartition xp = new XPartition();
    xp.setFactOrDimensionTableName("dim_table");
    xp.setLocation(new File("target/test-classes/dim2-part").getAbsolutePath());
    xp.setUpdatePeriod(XUpdatePeriod.HOURLY);
    XTimePartSpec timePart = new XTimePartSpec();
    XTimePartSpecElement partElement = new XTimePartSpecElement();
    partElement.setKey("dt");
    partElement.setValue(DatatypeFactory.newInstance().newXMLGregorianCalendar(new GregorianCalendar()));
    timePart.getPartSpecElement().add(partElement);
    xp.setTimePartitionSpec(timePart);
    result = client.addPartitionToDim("dim_table", "local", xp);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }


  @AfterTest
  public void tearDown() throws Exception {

    APIResult result = client.dropDimensionTable("dim_table", true);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    result = client.dropDimension("test_dim");
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    result = client.dropStorage("local");
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    result = client.dropCube("sample_cube");
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    result = client.dropDatabase(TEST_DB, true);
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

    result = client.closeConnection();
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
  }

  /**
   * Creates a new client and tests database creation and deletion
   */
  @Test
  public void testClient() throws Exception {
    LensClientConfig lensClientConfig = new LensClientConfig();
    lensClientConfig.setLensDatabase(TEST_DB);
    Assert.assertEquals(lensClientConfig.getLensDatabase(), TEST_DB);

    lensClientConfig.set(LensConfConstants.SERVER_BASE_URL, "http://localhost:" + getTestPort() + "/lensapi");
    LensClient client = new LensClient(lensClientConfig);
    Assert.assertEquals(client.getCurrentDatabae(), TEST_DB,
      "current database");

    client.createDatabase("testclientdb", true);
    Assert.assertTrue(client.getAllDatabases().contains("testclientdb"));
    client.dropDatabase("testclientdb", false);
    Assert.assertFalse(client.getAllDatabases().contains("testclientdb"));

    Assert.assertTrue(RequestTestFilter.isAccessed(), "RequestTestFilter not invoked");
  }

  @DataProvider(name = "testIterableHttpResultSetDP")
  public Object[][] testIterableHttpResultSetDP() {
    Object[][] testCases = new Object[7][];

    String query = "cube select id,name from test_dim";

    //**** Test server and driver Persist with Split and Header Enbaled
    testCases[0] = new Object[]{query, createConf(true, true, true, 1, true), true, 2, 3};

    //**** Test server and driver Persist with Split disabled and Header Enbaled
    testCases[1] = new Object[]{query, createConf(true, true, false, 0, true), false, 2, 3};

    //**** Test server and driver Persist with Split disabled and Header disabled
    testCases[2] = new Object[]{query, createConf(true, true, false, 0, false), false, 0, 3};

    //**** Test server Persist with Split enabled and Header enabled
    testCases[3] = new Object[]{query, createConf(false, true, true, 1, true), true, 2, 3};

    //**** Test server Persist with Split disabled and Header disabled
    testCases[4] = new Object[]{query, createConf(false, true, false, 0, false), false, 0, 3};

    String emptyQuery = "cube select id,name from test_dim where id = -999";
    //**** Test server and driver Persist with Split and Header Enbaled
    testCases[5] = new Object[]{emptyQuery, createConf(true, true, true, 1, true), true, 2, 0};

    //**** Test server and driver Persist with Split and Header disabled
    testCases[6] = new Object[]{emptyQuery, createConf(true, true, true, 1, false), true, 0, 0};

    return testCases;
  }

  private Map<String, String> createConf(boolean persistInDriver, boolean persistInServer, boolean split,
    int splitRows, boolean writeHeader) {
    Map<String, String> queryConf = new HashMap<String, String>();
    queryConf.put("lens.query.enable.persistent.resultset.indriver", "" + persistInDriver);
    queryConf.put("lens.query.enable.persistent.resultset", "" + persistInServer);
    queryConf.put("lens.query.result.split.multiple", "" + split);
    queryConf.put("lens.query.result.split.multiple.maxrows", "" + splitRows);
    queryConf.put("lens.query.output.write.header", "" + writeHeader);
    queryConf.put("lens.query.result.output.dir.format",
      "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'"
        + " WITH SERDEPROPERTIES ('serialization.null.format'='-NA-',"
        + " 'field.delim'=','  ) STORED AS TEXTFILE ");
    return queryConf;
  }

  @Test(dataProvider = "testIterableHttpResultSetDP")
  public void testHttpResultSet(String query, Map<String, String> queryConf, boolean isResultZipped,
    int columnNamesExpected, int rowsExpected) throws Exception {

    for (Map.Entry<String, String> e : queryConf.entrySet()) {
      client.setConnectionParam(e.getKey(), e.getValue());
    }
    QueryHandle handle = client.executeQueryAsynch(query, "testQuery");
    client.getStatement().waitForQueryToComplete(handle);
    assertTrue(client.getStatement().wasQuerySuccessful());

    ResultSet result = null;
    boolean isHeaderRowPresent = columnNamesExpected > 0 ? true : false;
    result = client.getHttpResultSet(handle);

    assertNotNull(result);
    validateResult(result, columnNamesExpected, rowsExpected);
  }

  private void validateResult(ResultSet result, int columnsExepected, int rowsExpected) throws LensClientIOException {
    if (columnsExepected > 0) {
      assertNotNull(result.getColumnNames());
      List columnNames = Arrays.asList(result.getColumnNames());
      compare(result.getColumnNames(), new String[]{"test_dim.id", "test_dim.name"});
    } else {
      assertNull(result.getColumnNames());
    }

    if (rowsExpected > 0) {
      assertTrue(result.next());
      compare(result.getRow(), new String[]{"1", "first"});
      assertTrue(result.next());
      compare(result.getRow(), new String[]{"2", "two"});
      assertTrue(result.next());
      compare(result.getRow(), new String[]{"3", "three"});
    }
  }

  private void compare(String[] actualArr, String[] expectedArr) {
    assertTrue(Arrays.equals(actualArr, expectedArr));
  }
}
