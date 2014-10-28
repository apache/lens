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

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.lens.cube.metadata.UpdatePeriod;
import org.apache.lens.cube.parse.BetweenTimeRangeWriter;
import org.apache.lens.cube.parse.StorageUtil;
import org.apache.lens.cube.parse.TimeRangeWriter;
import org.testng.Assert;

public class TestBetweenTimeRangeWriter extends TestTimeRangeWriter {

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
          getBetweenClause("test", "dt", CubeTestSetup.twodaysBack, CubeTestSetup.now, UpdatePeriod.DAILY.format());
    } else {
      expected = getBetweenClause("test", "dt", CubeTestSetup.twodaysBack, CubeTestSetup.now, format);
    }
    Assert.assertEquals(expected, whereClause);
  }

  public static String getBetweenClause(String alias, String colName, Date start, Date end, DateFormat format) {
    String first = format.format(start);
    String last = format.format(end);
    return " (" + alias + "." + colName + " BETWEEN '" + first + "' AND '" + last + "') ";
  }

  @Override
  public void validateSingle(String whereClause, DateFormat format) {
    List<String> parts = new ArrayList<String>();
    if (format == null) {
      parts.add(UpdatePeriod.DAILY.format().format(CubeTestSetup.oneDayBack));
    } else {
      parts.add(format.format(CubeTestSetup.oneDayBack));
    }

    System.out.println("Expected :" + StorageUtil.getWherePartClause("dt", "test", parts));
    Assert.assertEquals(whereClause, StorageUtil.getWherePartClause("dt", "test", parts));
  }
}
