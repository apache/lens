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
package org.apache.lens.server.query;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

import static org.apache.lens.api.error.LensCommonErrorCode.INTERNAL_SERVER_ERROR;

import static org.apache.lens.cube.error.LensCubeErrorCode.COLUMN_UNAVAILABLE_IN_TIME_RANGE;
import static org.apache.lens.cube.error.LensCubeErrorCode.SYNTAX_ERROR;

import static org.apache.lens.server.common.RestAPITestUtil.*;
import static org.apache.lens.server.common.TestDataUtils.*;
import static org.apache.lens.server.error.LensServerErrorCode.*;

import static org.testng.Assert.assertTrue;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.datatype.DatatypeConfigurationException;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.SupportedOperations;
import org.apache.lens.api.jaxb.LensJAXBContextResolver;
import org.apache.lens.api.metastore.*;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.api.result.LensErrorTO;
import org.apache.lens.api.util.MoxyJsonConfigurationContextResolver;
import org.apache.lens.cube.error.ColUnAvailableInTimeRange;
import org.apache.lens.cube.metadata.HDFSStorage;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.LensRequestLoggingFilter;
import org.apache.lens.server.common.ErrorResponseExpectedData;
import org.apache.lens.server.common.RestAPITestUtil;
import org.apache.lens.server.error.GenericExceptionMapper;
import org.apache.lens.server.error.LensJAXBValidationExceptionMapper;
import org.apache.lens.server.metastore.MetastoreResource;
import org.apache.lens.server.session.SessionResource;

import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.moxy.json.MoxyJsonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import lombok.NonNull;

@Test(groups = "unit-test")
public class QueryAPIErrorResponseTest extends LensJerseyTest {

  private static final String MOCK_QUERY = "mock-query";
  private static final String INVALID_OPERATION = "invalid-operation";

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
  }

  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Override
  protected Application configure() {

    enable(TestProperties.LOG_TRAFFIC);
    enable(TestProperties.DUMP_ENTITY);

    return new ResourceConfig(LensRequestLoggingFilter.class, SessionResource.class, MetastoreResource.class,
      QueryServiceResource.class, MultiPartFeature.class, GenericExceptionMapper.class,
      LensJAXBContextResolver.class,
      LensRequestLoggingFilter.class, LensJAXBValidationExceptionMapper.class,
      MoxyJsonConfigurationContextResolver.class, MoxyJsonFeature.class);
  }

  @Test(dataProvider = "mediaTypeData")
  public void testErrorResponseWhenSessionIdIsAbsent(MediaType mt) {

    Response response = estimate(target(), Optional.<LensSessionHandle>absent(), Optional.of(MOCK_QUERY), mt);

    final String expectedErrMsg = "Session id not provided. Please provide a session id.";
    LensErrorTO expectedLensErrorTO = LensErrorTO.composedOf(
        SESSION_ID_NOT_PROVIDED.getLensErrorInfo().getErrorCode(), expectedErrMsg, MOCK_STACK_TRACE);
    ErrorResponseExpectedData expectedData = new ErrorResponseExpectedData(BAD_REQUEST, expectedLensErrorTO);

    expectedData.verify(response);
  }

  @Test(dataProvider = "mediaTypeData")
  public void testErrorResponseWhenQueryIsAbsent(MediaType mt) {

    LensSessionHandle sessionId = openSession(target(), "foo", "bar", new LensConf(), mt);
    Optional<String> testQuery = Optional.absent();
    Response response = estimate(target(), Optional.of(sessionId), testQuery, mt);

    final String expectedErrMsg = "Query is not provided, or it is empty or blank. Please provide a valid query.";
    LensErrorTO expectedLensErrorTO = LensErrorTO.composedOf(
        NULL_OR_EMPTY_OR_BLANK_QUERY.getLensErrorInfo().getErrorCode(), expectedErrMsg, MOCK_STACK_TRACE);
    ErrorResponseExpectedData expectedData = new ErrorResponseExpectedData(BAD_REQUEST, expectedLensErrorTO);

    expectedData.verify(response);
    closeSession(target(), sessionId, mt);
  }

  @Test(dataProvider = "mediaTypeData")
  public void testErrorResponseWhenInvalidOperationIsSubmitted(MediaType mt) {

    LensSessionHandle sessionId = openSession(target(), "foo", "bar", new LensConf(), mt);

    Response response = postQuery(target(), Optional.of(sessionId), Optional.of(MOCK_QUERY),
      Optional.of(INVALID_OPERATION), mt);

    final String expectedErrMsg = "Provided Operation is not supported. Supported Operations are: "
      + "[estimate, execute, explain, execute_with_timeout]";

    LensErrorTO expectedLensErrorTO = LensErrorTO.composedOf(
        UNSUPPORTED_OPERATION.getLensErrorInfo().getErrorCode(),
      expectedErrMsg, MOCK_STACK_TRACE, new SupportedOperations());
    ErrorResponseExpectedData expectedData = new ErrorResponseExpectedData(BAD_REQUEST, expectedLensErrorTO);

    expectedData.verify(response);
    closeSession(target(), sessionId, mt);
  }

  @Test(dataProvider = "mediaTypeData")
  public void testErrorResponseWhenLensMultiCauseExceptionOccurs(MediaType mt) {

    LensSessionHandle sessionId = openSession(target(), "foo", "bar", mt);

    final String testQuery = "select * from non_existing_table";
    Response response = estimate(target(), Optional.of(sessionId), Optional.of(testQuery), mt);

    final String expectedErrMsg1 = "Semantic Error : Error while compiling statement: "
      + "FAILED: SemanticException [Error 10001]: Line 1:31 Table not found 'non_existing_table'";
    final String expectedErrMsg2 = "Semantic Error : user lacks privilege or object not found: NON_EXISTING_TABLE";

    LensErrorTO expectedLensErrorTO1 = LensErrorTO.composedOf(INTERNAL_SERVER_ERROR.getValue(),
            expectedErrMsg1, MOCK_STACK_TRACE);

    LensErrorTO expectedLensErrorTO2 = LensErrorTO.composedOf(INTERNAL_SERVER_ERROR.getValue(),
            expectedErrMsg2, MOCK_STACK_TRACE);

    LensErrorTO responseLensErrorTO = response.readEntity(LensAPIResult.class).getLensErrorTO();

    assertTrue(expectedLensErrorTO1.getMessage().equals(responseLensErrorTO.getMessage())
        || expectedLensErrorTO2.getMessage().equals(responseLensErrorTO.getMessage()),
      "Message is " + responseLensErrorTO.getMessage());
    closeSession(target(), sessionId, mt);

  }

  @Test(dataProvider = "mediaTypeData")
  public void testErrorResponseWithSyntaxErrorInQuery(MediaType mt) {

    LensSessionHandle sessionId = openSession(target(), "foo", "bar", new LensConf(), mt);

    Response response = estimate(target(), Optional.of(sessionId), Optional.of(MOCK_QUERY), mt);

    final String expectedErrMsg = "Syntax Error: line 1:0 cannot recognize input near 'mock' '-' 'query'";
    LensErrorTO expectedLensErrorTO = LensErrorTO.composedOf(SYNTAX_ERROR.getLensErrorInfo().getErrorCode(),
      expectedErrMsg, MOCK_STACK_TRACE);
    ErrorResponseExpectedData expectedData = new ErrorResponseExpectedData(BAD_REQUEST, expectedLensErrorTO);

    expectedData.verify(response);
    closeSession(target(), sessionId, mt);
  }

  @Test(dataProvider = "mediaTypeData")
  public void testQueryColumnWithBothStartDateAndEndDate(MediaType mt) throws DatatypeConfigurationException {

    /* This test will have a col which has both start date and end date set */
    /* Col will be queried for a time range which does not fall in start date and end date */

    DateTime startDateOneJan2015 = new DateTime(2015, 01, 01, 0, 0, DateTimeZone.UTC);
    DateTime endDateThirtyJan2015 = new DateTime(2015, 01, 30, 23, 0, DateTimeZone.UTC);

    DateTime queryFromOneJan2014 = new DateTime(2014, 01, 01, 0, 0, DateTimeZone.UTC);
    DateTime queryTillThreeJan2014 = new DateTime(2014, 01, 03, 0, 0, DateTimeZone.UTC);

    final String expectedErrMsgSuffix = " can only be queried after Thursday, January 1, 2015 12:00:00 AM UTC and "
      + "before Friday, January 30, 2015 11:00:00 PM UTC. Please adjust the selected time range accordingly.";

    testColUnAvailableInTimeRange(Optional.of(startDateOneJan2015),
      Optional.of(endDateThirtyJan2015), queryFromOneJan2014, queryTillThreeJan2014, expectedErrMsgSuffix, mt);
  }

  @Test(dataProvider = "mediaTypeData")
  public void testQueryColumnWithOnlyStartDate(MediaType mt) throws DatatypeConfigurationException {

    /* This test will have a col which has only start date set */
    /* Col will be queried for a time range which is before start date */

    DateTime startDateOneJan2015 = new DateTime(2015, 01, 01, 0, 0, DateTimeZone.UTC);

    DateTime queryFromOneJan2014 = new DateTime(2014, 01, 01, 0, 0, DateTimeZone.UTC);
    DateTime queryTillThreeJan2014 = new DateTime(2014, 01, 03, 0, 0, DateTimeZone.UTC);

    final String expectedErrMsgSuffix = " can only be queried after Thursday, January 1, 2015 12:00:00 AM UTC. "
      + "Please adjust the selected time range accordingly.";

    testColUnAvailableInTimeRange(Optional.of(startDateOneJan2015),
      Optional.<DateTime>absent(), queryFromOneJan2014, queryTillThreeJan2014, expectedErrMsgSuffix, mt);
  }

  @Test(dataProvider = "mediaTypeData")
  public void testQueryColumnWithOnlyEndDate(MediaType mt) throws DatatypeConfigurationException {

    /* This test will have a col which has only end date set */
    /* Col will be queried for a time range which is after end date */

    DateTime endDateThirtyJan2015 = new DateTime(2015, 01, 30, 23, 0, DateTimeZone.UTC);

    DateTime queryFromOneJan2016 = new DateTime(2016, 01, 01, 0, 0, DateTimeZone.UTC);
    DateTime queryTillThreeJan2016 = new DateTime(2016, 01, 03, 0, 0, DateTimeZone.UTC);

    final String expectedErrMsgSuffix = " can only be queried before Friday, January 30, 2015 11:00:00 PM UTC. "
      + "Please adjust the selected time range accordingly.";

    testColUnAvailableInTimeRange(Optional.<DateTime>absent(),
      Optional.of(endDateThirtyJan2015), queryFromOneJan2016, queryTillThreeJan2016, expectedErrMsgSuffix, mt);
  }

  private void testColUnAvailableInTimeRange(@NonNull final Optional<DateTime> colStartDate,
    @NonNull final Optional<DateTime> colEndDate, @NonNull DateTime queryFrom, @NonNull DateTime queryTill,
    @NonNull final String expectedErrorMsgSuffix, @NonNull final MediaType mt) throws DatatypeConfigurationException {

    final WebTarget target = target();
    final String testDb = getRandomDbName();
    final String testCube = getRandomCubeName();
    final String testDimensionField = getRandomDimensionField();
    final String testFact = getRandomFactName();
    final String testStorage = getRandomStorageName();

    /* Setup: Begin */
    LensSessionHandle sessionId = openSession(target, "foo", "bar", new LensConf(), mt);

    try {

      createAndSetCurrentDbFailFast(target, sessionId, testDb, mt);

      /* Create a test cube with test dimension field having a start Date and end Date */
      XDimAttribute testXDim = createXDimAttribute(testDimensionField, colStartDate, colEndDate);
      XCube xcube = createXCubeWithDummyMeasure(testCube, Optional.of("dt"), testXDim);
      createCubeFailFast(target, sessionId, xcube, mt);

      /* Create Storage */
      XStorage xs = new XStorage();
      xs.setClassname(HDFSStorage.class.getCanonicalName());
      xs.setName(testStorage);
      RestAPITestUtil.createStorageFailFast(target, sessionId, xs, mt);

      /* Create a fact with test dimension field */
      XColumn xColumn = createXColumn(testDimensionField);
      XFactTable xFactTable = createXFactTableWithColumns(testFact, testCube, xColumn);

      //Create a StorageTable
      XStorageTables tables = new XStorageTables();
      tables.getStorageTable().add(createStorageTblElement(testStorage, "DAILY"));
      tables.getStorageTable().add(createStorageTblElement("mydb", "DAILY")); // for jdbc
      xFactTable.setStorageTables(tables);

      createFactFailFast(target, sessionId, xFactTable, mt);

      /* Setup: End */

      DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd-HH");
      final String testQuery = "cube select " + testDimensionField + " from " + testCube + " where TIME_RANGE_IN(dt, "
        + "\"" + dtf.print(queryFrom) + "\",\"" + dtf.print(queryTill) + "\")";

      Response response = estimate(target, Optional.of(sessionId), Optional.of(testQuery), mt);

      final String expectedErrMsg = testDimensionField + expectedErrorMsgSuffix;

      Long expecAvailableFrom = colStartDate.isPresent() ? colStartDate.get().getMillis() : null;
      Long expecAvailableTill = colEndDate.isPresent() ? colEndDate.get().getMillis() : null;

      final ColUnAvailableInTimeRange expectedErrorPayload = new ColUnAvailableInTimeRange(testDimensionField,
        expecAvailableFrom, expecAvailableTill);

      LensErrorTO expectedLensErrorTO = LensErrorTO.composedOf(
          COLUMN_UNAVAILABLE_IN_TIME_RANGE.getLensErrorInfo().getErrorCode(),
          expectedErrMsg, MOCK_STACK_TRACE, expectedErrorPayload, null);
      ErrorResponseExpectedData expectedData = new ErrorResponseExpectedData(BAD_REQUEST, expectedLensErrorTO);

      expectedData.verify(response);
    } finally {
      dropDatabaseFailFast(target, sessionId, testDb, mt);
      closeSessionFailFast(target, sessionId, mt);
    }
  }

  /**
   * Test execute failure in with selected driver throwing Runtime exception.
   *
   * @throws InterruptedException the interrupted exception
   */
  @Test(dataProvider = "mediaTypeData")
  public void testExplainRuntimeException(MediaType mt) throws InterruptedException {
    LensSessionHandle sessionId = openSession(target(), "foo", "bar", new LensConf(), mt);
    try {
      Response response = explain(target(), Optional.of(sessionId), Optional.of("select fail, execute_runtime "
        + " from non_exist"), mt);
      final String expectedErrMsg = "Internal server error:Runtime exception from query explain";
      LensErrorTO expectedLensErrorTO = LensErrorTO.composedOf(
        INTERNAL_SERVER_ERROR.getValue(), expectedErrMsg, MOCK_STACK_TRACE);
      ErrorResponseExpectedData expectedData = new ErrorResponseExpectedData(Response.Status.INTERNAL_SERVER_ERROR,
        expectedLensErrorTO);
      expectedData.verify(response);
    } finally {
      closeSessionFailFast(target(), sessionId, mt);
    }

  }
  /**
   * Test execute failure in with selected driver throwing webapp exception.
   *
   * @throws InterruptedException the interrupted exception
   */
  @Test(dataProvider = "mediaTypeData")
  public void testExplainWebappException(MediaType mt) throws InterruptedException {
    LensSessionHandle sessionId = openSession(target(), "foo", "bar", new LensConf(), mt);
    try {
      Response response = explain(target(), Optional.of(sessionId), Optional.of("select fail, webappexception "
        + " from non_exist"), mt);
      final String expectedErrMsg = "Not found from mock driver";
      LensErrorTO expectedLensErrorTO = LensErrorTO.composedOf(
        NOT_FOUND.getStatusCode(), expectedErrMsg, MOCK_STACK_TRACE);
      ErrorResponseExpectedData expectedData = new ErrorResponseExpectedData(Response.Status.NOT_FOUND,
        expectedLensErrorTO);
      expectedData.verify(response);
    } finally {
      closeSessionFailFast(target(), sessionId, mt);
    }
  }

  private XStorageTableElement createStorageTblElement(String storageName, String... updatePeriod) {
    XStorageTableElement tbl = new XStorageTableElement();
    tbl.setUpdatePeriods(new XUpdatePeriods());
    tbl.setStorageName(storageName);
    if (updatePeriod != null) {
      for (String p : updatePeriod) {
        tbl.getUpdatePeriods().getUpdatePeriod().add(XUpdatePeriod.valueOf(p));
      }
    }
    tbl.setTableDesc(new XStorageTableDesc());
    return tbl;
  }
}
