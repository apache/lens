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
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.lens.cube.metadata.UpdatePeriod;
import org.apache.lens.cube.parse.FactPartition;
import org.apache.lens.cube.parse.TimeRangeWriter;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public abstract class TestTimeRangeWriter {

  public abstract TimeRangeWriter getTimerangeWriter();

  public abstract boolean failDisjoint();

  public abstract void validateDisjoint(String whereClause, DateFormat format);

  public abstract void validateConsecutive(String whereClause, DateFormat format);

  public abstract void validateSingle(String whereClause, DateFormat object);

  public static DateFormat dbFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  @BeforeTest
  public void setup() {
    CubeTestSetup.init();
  }

  @Test
  public void testDisjointParts() {
    Set<FactPartition> answeringParts = new LinkedHashSet<FactPartition>();
    answeringParts.add(new FactPartition("dt", CubeTestSetup.twoMonthsBack, UpdatePeriod.MONTHLY, null, null));
    answeringParts.add(new FactPartition("dt", CubeTestSetup.twodaysBack, UpdatePeriod.DAILY, null, null));
    answeringParts.add(new FactPartition("dt", CubeTestSetup.now, UpdatePeriod.HOURLY, null, null));

    SemanticException th = null;
    String whereClause = null;
    try {
      whereClause = getTimerangeWriter().getTimeRangeWhereClause(null, "test", answeringParts);
    } catch (SemanticException e) {
      e.printStackTrace();
      th = e;
    }

    if (failDisjoint()) {
      Assert.assertNotNull(th);
      Assert
          .assertEquals(th.getCanonicalErrorMsg().getErrorCode(), ErrorMsg.CANNOT_USE_TIMERANGE_WRITER.getErrorCode());
    } else {
      Assert.assertNull(th);
      validateDisjoint(whereClause, null);
    }

    // test with format
    answeringParts = new LinkedHashSet<FactPartition>();
    answeringParts.add(new FactPartition("dt", CubeTestSetup.twoMonthsBack, UpdatePeriod.MONTHLY, null, dbFormat));
    answeringParts.add(new FactPartition("dt", CubeTestSetup.twodaysBack, UpdatePeriod.DAILY, null, dbFormat));
    answeringParts.add(new FactPartition("dt", CubeTestSetup.now, UpdatePeriod.HOURLY, null, dbFormat));

    th = null;
    try {
      whereClause = getTimerangeWriter().getTimeRangeWhereClause(null, "test", answeringParts);
    } catch (SemanticException e) {
      th = e;
    }

    if (failDisjoint()) {
      Assert.assertNotNull(th);
    } else {
      Assert.assertNull(th);
      validateDisjoint(whereClause, dbFormat);
    }

  }

  @Test
  public void testConsecutiveDayParts() throws SemanticException {
    Set<FactPartition> answeringParts = new LinkedHashSet<FactPartition>();
    answeringParts.add(new FactPartition("dt", CubeTestSetup.oneDayBack, UpdatePeriod.DAILY, null, null));
    answeringParts.add(new FactPartition("dt", CubeTestSetup.twodaysBack, UpdatePeriod.DAILY, null, null));
    answeringParts.add(new FactPartition("dt", CubeTestSetup.now, UpdatePeriod.DAILY, null, null));

    String whereClause = getTimerangeWriter().getTimeRangeWhereClause(null, "test", answeringParts);
    validateConsecutive(whereClause, null);

    answeringParts = new LinkedHashSet<FactPartition>();
    answeringParts.add(new FactPartition("dt", CubeTestSetup.oneDayBack, UpdatePeriod.DAILY, null, dbFormat));
    answeringParts.add(new FactPartition("dt", CubeTestSetup.twodaysBack, UpdatePeriod.DAILY, null, dbFormat));
    answeringParts.add(new FactPartition("dt", CubeTestSetup.now, UpdatePeriod.DAILY, null, dbFormat));

    whereClause = getTimerangeWriter().getTimeRangeWhereClause(null, "test", answeringParts);
    validateConsecutive(whereClause, dbFormat);
  }

  @Test
  public void testSinglePart() throws SemanticException {
    Set<FactPartition> answeringParts = new LinkedHashSet<FactPartition>();
    answeringParts.add(new FactPartition("dt", CubeTestSetup.oneDayBack, UpdatePeriod.DAILY, null, null));
    String whereClause = getTimerangeWriter().getTimeRangeWhereClause(null, "test", answeringParts);
    validateSingle(whereClause, null);

    answeringParts = new LinkedHashSet<FactPartition>();
    answeringParts.add(new FactPartition("dt", CubeTestSetup.oneDayBack, UpdatePeriod.DAILY, null, dbFormat));
    whereClause = getTimerangeWriter().getTimeRangeWhereClause(null, "test", answeringParts);
    validateSingle(whereClause, dbFormat);

  }
}
