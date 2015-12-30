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

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.lens.cube.metadata.UpdatePeriod;

import org.testng.Assert;

public class TestORTimeRangeWriter extends TestTimeRangeWriter {

  @Override
  public TimeRangeWriter getTimerangeWriter() {
    return new ORTimeRangeWriter();
  }

  @Override
  public boolean failDisjoint() {
    return false;
  }

  @Override
  public void validateDisjoint(String whereClause, DateFormat format) {
    List<String> parts = new ArrayList<String>();
    if (format == null) {
      parts.add(UpdatePeriod.MONTHLY.format(TWO_MONTHS_BACK));
      parts.add(UpdatePeriod.DAILY.format(TWODAYS_BACK));
      parts.add(UpdatePeriod.HOURLY.format(NOW));
    } else {
      parts.add(format.format(TWO_MONTHS_BACK));
      parts.add(format.format(TWODAYS_BACK));
      parts.add(format.format(NOW));
    }

    System.out.println("Expected :" + StorageUtil.getWherePartClause("dt", "test", parts));
    Assert.assertEquals(whereClause, StorageUtil.getWherePartClause("dt", "test", parts));
  }

  @Override
  public void validateConsecutive(String whereClause, DateFormat format) {
    List<String> parts = new ArrayList<String>();
    if (format == null) {
      parts.add(getDateStringWithOffset(UpdatePeriod.DAILY, -1));
      parts.add(getDateStringWithOffset(UpdatePeriod.DAILY, -2));
      parts.add(getDateStringWithOffset(UpdatePeriod.DAILY, 0));
    } else {
      parts.add(format.format(getDateWithOffset(UpdatePeriod.DAILY, -1)));
      parts.add(format.format(getDateWithOffset(UpdatePeriod.DAILY, -2)));
      parts.add(format.format(getDateWithOffset(UpdatePeriod.DAILY, 0)));
    }

    System.out.println("Expected :" + StorageUtil.getWherePartClause("dt", "test", parts));
    Assert.assertEquals(whereClause, StorageUtil.getWherePartClause("dt", "test", parts));
  }
}
