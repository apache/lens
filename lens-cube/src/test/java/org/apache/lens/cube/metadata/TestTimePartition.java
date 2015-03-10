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
package org.apache.lens.cube.metadata;

import java.util.Date;

import org.apache.lens.api.LensException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestTimePartition {
  @Test
  public void test() throws LensException {
    Date now = new Date();
    for (UpdatePeriod up : UpdatePeriod.values()) {
      String nowStr = up.format().format(now);
      TimePartition nowPartition = TimePartition.of(up, now);
      TimePartition nowStrPartition = TimePartition.of(up, nowStr);
      Assert.assertEquals(nowPartition, nowStrPartition);
      Assert.assertTrue(nowPartition.next().after(nowPartition));
      Assert.assertTrue(nowPartition.previous().before(nowPartition));
      Assert.assertEquals(getLensExceptionFromPartitionParsing(up, "garbage").getMessage(),
        TimePartition.getWrongUpdatePeriodMessage(up, "garbage"));
      getLensExceptionFromPartitionParsing(up, (Date) null);
      getLensExceptionFromPartitionParsing(up, (String) null);
      getLensExceptionFromPartitionParsing(up, "");

      // parse with other update periods
      for (UpdatePeriod up2 : UpdatePeriod.values()) {
        // handles the equality case and the case where monthly-quarterly have same format strings.
        if (up.formatStr().equals(up2.formatStr())) {
          continue;
        }
        Assert.assertEquals(getLensExceptionFromPartitionParsing(up2, nowStr).getMessage(),
          TimePartition.getWrongUpdatePeriodMessage(up2, nowStr));
      }
    }
  }

  private LensException getLensExceptionFromPartitionParsing(UpdatePeriod up, String dateStr) {
    try {
      TimePartition.of(up, dateStr);
      Assert.fail("Should have thrown LensException");
    } catch (LensException e) {
      return e;
    }
    return null; // redundant
  }

  private LensException getLensExceptionFromPartitionParsing(UpdatePeriod up, Date date) {
    try {
      TimePartition.of(up, date);
      Assert.fail("Should have thrown LensException");
    } catch (LensException e) {
      return e;
    }
    return null; // redundant
  }
}
