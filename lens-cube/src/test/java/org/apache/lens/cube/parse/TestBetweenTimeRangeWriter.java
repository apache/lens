/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lens.cube.parse;

import static org.apache.lens.cube.metadata.DateFactory.*;
import static org.apache.lens.cube.metadata.UpdatePeriod.DAILY;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.server.api.error.LensException;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestBetweenTimeRangeWriter extends TestTimeRangeWriter {

  public static final String CLOSED = "CLOSED";
  public static final String OPEN = "OPEN";

  public static final int START_DATE_OFFSET = 1;
  public static final int END_DATE_OFFSET = 2;

  public static final DateFormat DAY_DB_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

  private static Set<FactPartition> answeringParts;
  private static Set<FactPartition> answeringPartsWithFormat;

  static {
    answeringParts = new LinkedHashSet<FactPartition>();
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, START_DATE_OFFSET), DAILY, null, null));
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, END_DATE_OFFSET), DAILY, null, null));

    answeringPartsWithFormat = new LinkedHashSet<>();
    answeringPartsWithFormat.add(new FactPartition("dt", getDateWithOffset(DAILY, START_DATE_OFFSET), DAILY, null,
      DAY_DB_FORMAT));
    answeringPartsWithFormat.add(new FactPartition("dt", getDateWithOffset(DAILY, END_DATE_OFFSET), DAILY, null,
      DAY_DB_FORMAT));

  }
  @Override
  public TimeRangeWriter getTimerangeWriter() {
    return new BetweenTimeRangeWriter();
  }

  @Override
  public boolean failDisjoint() {
    return true;
  }

  @Override
  public void validateDisjoint(String whereClause, DateFormat format) {
    Assert.fail();
  }

  @Override
  public void validateConsecutive(String whereClause, DateFormat format) {
    String expected = null;
    if (format == null) {
      expected =
        getBetweenClause("test", "dt", TWODAYS_BACK, NOW, DAILY.format());
    } else {
      expected = getBetweenClause("test", "dt", TWODAYS_BACK, NOW, format);
    }
    Assert.assertEquals(expected, whereClause);
  }

  public static String getBetweenClause(String alias, String colName, Date start, Date end, DateFormat format) {
    String first = format.format(start);
    String last = format.format(end);
    return " (" + alias + "." + colName + " BETWEEN '" + first + "' AND '" + last + "') ";
  }

  @Test
  public void testSinglePartBetweenOnly() throws LensException {
    Set<FactPartition> answeringParts = new LinkedHashSet<FactPartition>();
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, -1), DAILY, null, null));
    String whereClause = getTimerangeWriter().getTimeRangeWhereClause(getMockedCubeContext(true), "test",
      answeringParts);
    validateBetweenOnlySingle(whereClause, null);

    answeringParts = new LinkedHashSet<>();
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, -1), DAILY, null, DB_FORMAT));
    whereClause = getTimerangeWriter().getTimeRangeWhereClause(getMockedCubeContext(true), "test", answeringParts);
    validateBetweenOnlySingle(whereClause, DB_FORMAT);

  }

  public void validateBetweenOnlySingle(String whereClause, DateFormat format) {
    String expected = null;
    if (format == null) {
      expected =
        getBetweenClause("test", "dt", getDateWithOffset(DAILY, -1), getDateWithOffset(DAILY, -1), DAILY.format());
    } else {
      expected = getBetweenClause("test", "dt", getDateWithOffset(DAILY, -1), getDateWithOffset(DAILY, -1), format);
    }
    Assert.assertEquals(expected, whereClause);
  }

  @DataProvider
  public Object[][] getBoundTypes() {

    Object[][] data = new Object[4][2];

    data[0][0] = OPEN;
    data[0][1] = OPEN;

    data[1][0] = OPEN;
    data[1][1] = CLOSED;

    data[2][0] = CLOSED;
    data[2][1] = OPEN;

    data[3][0] = CLOSED;
    data[3][1] = CLOSED;

    return data;
  }

  @Test(dataProvider = "getBoundTypes")
  public void testBetweenBoundTypes(String startBoundType, String endBoundType)
    throws LensException {
    String whereClause =
      getTimerangeWriter().getTimeRangeWhereClause(getMockedCubeContextForBounds(startBoundType, endBoundType),
        "test", answeringParts);

    int testStartOffset = START_DATE_OFFSET;
    int testEndOffset = END_DATE_OFFSET;
    if (startBoundType.equals(OPEN)) {
      testStartOffset = START_DATE_OFFSET - 1;
    }
    if (endBoundType.equals(OPEN)) {
      testEndOffset = END_DATE_OFFSET + 1;
    }
    validateBetweenBoundTypes(whereClause, null, testStartOffset, testEndOffset);

    whereClause =
      getTimerangeWriter().getTimeRangeWhereClause(getMockedCubeContextForBounds(startBoundType, endBoundType),
        "test", answeringPartsWithFormat);

    validateBetweenBoundTypes(whereClause, DAY_DB_FORMAT, testStartOffset, testEndOffset);
  }

  private void validateBetweenBoundTypes(String whereClause, DateFormat format, int testStartOffset, int testEndOffset)
  {
    String expected = null;
    if (format == null) {
      expected =
        getBetweenClause("test", "dt", getDateWithOffset(DAILY, testStartOffset),
          getDateWithOffset(DAILY, testEndOffset), DAILY.format());
    } else {
      expected =
        getBetweenClause("test", "dt", getDateWithOffset(DAILY, testStartOffset),
          getDateWithOffset(DAILY, testEndOffset), format);
    }
    Assert.assertEquals(expected, whereClause);
  }
}
