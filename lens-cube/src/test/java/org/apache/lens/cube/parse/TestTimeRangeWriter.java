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

package org.apache.lens.cube.parse;

import static org.apache.lens.cube.metadata.DateFactory.*;
import static org.apache.lens.cube.metadata.UpdatePeriod.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class TestTimeRangeWriter {

  public abstract TimeRangeWriter getTimerangeWriter();

  public abstract boolean failDisjoint();

  public abstract void validateDisjoint(String whereClause, DateFormat format);

  public abstract void validateConsecutive(String whereClause, DateFormat format);

  protected CubeQueryContext getMockedCubeContext(boolean betweenOnly) {
    CubeQueryContext context = Mockito.mock(CubeQueryContext.class);
    Configuration configuration = new Configuration();
    configuration.setBoolean(CubeQueryConfUtil.BETWEEN_ONLY_TIME_RANGE_WRITER, betweenOnly);
    Mockito.when(context.getConf()).thenReturn(configuration);
    Mockito.when(context.shouldReplaceTimeDimWithPart()).thenReturn(true);
    return context;
  }

  public void validateSingle(String whereClause, DateFormat format) {
    List<String> parts = new ArrayList<String>();
    if (format == null) {
      parts.add(getDateStringWithOffset(DAILY, -1));
    } else {
      parts.add(format.format(getDateWithOffset(DAILY, -1)));
    }

    System.out.println("Expected :" + StorageUtil.getWherePartClause("dt", "test", parts));
    Assert.assertEquals(whereClause, StorageUtil.getWherePartClause("dt", "test", parts));
  }

  public static final DateFormat DB_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  @Test
  public void testDisjointParts() {
    Set<FactPartition> answeringParts = new LinkedHashSet<FactPartition>();
    answeringParts.add(new FactPartition("dt", getDateWithOffset(MONTHLY, -2), MONTHLY, null, null));
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, -2), DAILY, null, null));
    answeringParts.add(new FactPartition("dt", getDateWithOffset(HOURLY, 0), HOURLY, null, null));

    LensException th = null;
    String whereClause = null;
    try {
      whereClause = getTimerangeWriter().getTimeRangeWhereClause(getMockedCubeContext(false), "test", answeringParts);
    } catch (LensException e) {
      log.error("Semantic exception while testing disjoint parts.", e);
      th = e;
    }

    if (failDisjoint()) {
      Assert.assertNotNull(th);
      Assert
        .assertEquals(th.getErrorCode(), LensCubeErrorCode.CANNOT_USE_TIMERANGE_WRITER.
            getLensErrorInfo().getErrorCode());
    } else {
      Assert.assertNull(th);
      validateDisjoint(whereClause, null);
    }

    // test with format
    answeringParts = new LinkedHashSet<>();
    answeringParts.add(new FactPartition("dt", getDateWithOffset(MONTHLY, -2), MONTHLY, null, DB_FORMAT));
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, -2), DAILY, null, DB_FORMAT));
    answeringParts.add(new FactPartition("dt", getDateWithOffset(HOURLY, 0), HOURLY, null, DB_FORMAT));

    th = null;
    try {
      whereClause = getTimerangeWriter().getTimeRangeWhereClause(getMockedCubeContext(false), "test", answeringParts);
    } catch (LensException e) {
      th = e;
    }

    if (failDisjoint()) {
      Assert.assertNotNull(th);
    } else {
      Assert.assertNull(th);
      validateDisjoint(whereClause, DB_FORMAT);
    }

  }
  @DataProvider
  public Object[][] formatDataProvider() {
    return new Object[][] {
      {null, },
      {DB_FORMAT, },
    };
  }
  @Test(dataProvider = "formatDataProvider")
  public void testConsecutiveDayParts(DateFormat format) throws LensException, InterruptedException {
    Set<FactPartition> answeringParts = new LinkedHashSet<>();
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, -1), DAILY, null, format));
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, -2), DAILY, null, format));
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, 0), DAILY, null, format));

    String whereClause = getTimerangeWriter().getTimeRangeWhereClause(getMockedCubeContext(false), "test",
      answeringParts);
    validateConsecutive(whereClause, format);
  }

  @Test
  public void testSinglePart() throws LensException {
    Set<FactPartition> answeringParts = new LinkedHashSet<FactPartition>();
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, -1), DAILY, null, null));
    String whereClause = getTimerangeWriter().getTimeRangeWhereClause(getMockedCubeContext(false), "test",
      answeringParts);
    validateSingle(whereClause, null);

    answeringParts = new LinkedHashSet<>();
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, -1), DAILY, null, DB_FORMAT));
    whereClause = getTimerangeWriter().getTimeRangeWhereClause(getMockedCubeContext(false), "test", answeringParts);
    validateSingle(whereClause, DB_FORMAT);

  }

}
