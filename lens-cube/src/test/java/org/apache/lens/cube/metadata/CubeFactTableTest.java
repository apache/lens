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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.lens.server.api.error.LensException;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CubeFactTableTest {
  Date now = new Date();

  @DataProvider(name = "properties")
  public Object[][] factProperties() throws LensException {
    String minus1DaysRelative = "now -1 days";
    String minus2DaysRelative = "now -2 days";
    String plus1DaysRelative = "now +1 days";
    String plus2DaysRelative = "now +2 days";

    String minus1DaysAbsolute = DateUtil.relativeToAbsolute(minus1DaysRelative, now);
    String minus2DaysAbsolute = DateUtil.relativeToAbsolute(minus2DaysRelative, now);
    String plus1DaysAbsolute = DateUtil.relativeToAbsolute(plus1DaysRelative, now);
    String plus2DaysAbsolute = DateUtil.relativeToAbsolute(plus2DaysRelative, now);

    Date minus1DaysDate = DateUtil.resolveRelativeDate(minus1DaysRelative, now);
    Date minus2DaysDate = DateUtil.resolveRelativeDate(minus2DaysRelative, now);
    Date plus1DaysDate = DateUtil.resolveRelativeDate(plus1DaysRelative, now);
    Date plus2DaysDate = DateUtil.resolveRelativeDate(plus2DaysRelative, now);

    return new Object[][]{
      {null, null, null, null, new Date(Long.MIN_VALUE), new Date(Long.MAX_VALUE)},
      {null, minus2DaysRelative, null, plus2DaysRelative, minus2DaysDate, plus2DaysDate},
      {minus2DaysAbsolute, null, plus2DaysAbsolute, null, minus2DaysDate, plus2DaysDate},
      {minus1DaysAbsolute, minus2DaysRelative, plus1DaysAbsolute, plus2DaysRelative, minus1DaysDate, plus1DaysDate},
      {minus2DaysAbsolute, minus1DaysRelative, plus2DaysAbsolute, plus1DaysRelative, minus1DaysDate, plus1DaysDate},
    };
  }

  private CubeFactTable getMockCubeFactTable(Map<String, String> properties) {
    CubeFactTable cubeFactTable = mock(CubeFactTable.class);
    when(cubeFactTable.now()).thenReturn(now);

    when(cubeFactTable.getProperties()).thenReturn(properties);

    when(cubeFactTable.getRelativeStartTime()).thenCallRealMethod();
    when(cubeFactTable.getAbsoluteStartTime()).thenCallRealMethod();

    when(cubeFactTable.getRelativeEndTime()).thenCallRealMethod();
    when(cubeFactTable.getAbsoluteEndTime()).thenCallRealMethod();

    when(cubeFactTable.getStartTime()).thenCallRealMethod();
    when(cubeFactTable.getEndTime()).thenCallRealMethod();

    return cubeFactTable;
  }

  @Test(dataProvider = "properties")
  public void testStartAndEndTime(String absoluteStartProperty, String relativeStartProperty,
    String absoluteEndProperty, String relativeEndProperty,
    Date expectedStartTime, Date expectedEndTime) throws Exception {

    Map<String, String> properties = new HashMap<>();
    if (absoluteStartProperty != null) {
      properties.put(MetastoreConstants.FACT_ABSOLUTE_START_TIME, absoluteStartProperty);
    }
    if (relativeStartProperty != null) {
      properties.put(MetastoreConstants.FACT_RELATIVE_START_TIME, relativeStartProperty);
    }
    if (absoluteEndProperty != null) {
      properties.put(MetastoreConstants.FACT_ABSOLUTE_END_TIME, absoluteEndProperty);
    }
    if (relativeEndProperty != null) {
      properties.put(MetastoreConstants.FACT_RELATIVE_END_TIME, relativeEndProperty);
    }
    CubeFactTable cubeFactTable = getMockCubeFactTable(properties);

    assertEquals(cubeFactTable.getStartTime(), expectedStartTime);
    assertEquals(cubeFactTable.getEndTime(), expectedEndTime);
  }
}
