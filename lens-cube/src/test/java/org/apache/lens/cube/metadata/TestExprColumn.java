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

import static org.testng.Assert.*;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.ExprColumn.ExprSpec;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import org.testng.annotations.Test;

public class TestExprColumn {

  private final Date now;
  private final Date twoDaysBack;
  private final Date nowUptoHours;
  private final Date twoDaysBackUptoHours;

  public TestExprColumn() {
    Calendar cal = Calendar.getInstance();
    cal.setTimeZone(TimeZone.getTimeZone("UTC"));
    now = cal.getTime();
    cal.add(Calendar.DAY_OF_MONTH, -2);
    twoDaysBack = cal.getTime();
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    twoDaysBackUptoHours = cal.getTime();
    cal.add(Calendar.DAY_OF_MONTH, +2);
    nowUptoHours = cal.getTime();
  }

  @Test
  public void testExprColumnEquality() throws Exception {
    FieldSchema colSchema = new FieldSchema("someExprCol", "double", "some exprcol");

    ExprColumn col1 = new ExprColumn(colSchema, "someExprDisplayString", "AVG(msr1) + AVG(msr2)");
    ExprColumn col2 = new ExprColumn(colSchema, "someExprDisplayString", "avg(MSR1) + avg(MSR2)");
    assertEquals(col1, col2);
    assertEquals(col1.hashCode(), col2.hashCode());

    col2 = new ExprColumn(colSchema, "someExprDisplayString", new ExprSpec("avg(MSR1) + avg(MSR2)", null, null));
    assertEquals(col1, col2);
    assertEquals(col1.hashCode(), col2.hashCode());

    ExprColumn col3 = new ExprColumn(colSchema, "someExprDisplayString", "AVG(msr1)");
    assertNotEquals(col1, col3);
    assertNotEquals(col1.hashCode(), col3.hashCode());

    ExprColumn col4 = new ExprColumn(colSchema, "someExprDisplayString", "dim1 = 'FooBar' AND dim2 = 'BarFoo'");
    ExprColumn col5 = new ExprColumn(colSchema, "someExprDisplayString", "dim1 = 'FOOBAR' AND dim2 = 'BarFoo'");
    assertNotEquals(col4.hashCode(), col5.hashCode());
    assertNotEquals(col4, col5);

    ExprColumn col6 = new ExprColumn(colSchema, "someExprDisplayString", "DIM1 = 'FooBar' AND DIM2 = 'BarFoo'");
    assertEquals(col4, col6);
    assertEquals(col4.hashCode(), col6.hashCode());
  }

  @Test
  public void testExpressionStartAndEndTimes() throws Exception {
    FieldSchema colSchema = new FieldSchema("multiExprCol", "double", "multi exprcol");

    ExprColumn col1 = new ExprColumn(colSchema, "multiExprColDisplayString", new ExprSpec("avg(MSR1) + avg(MSR2)",
      twoDaysBack, null));
    Map<String, String> props = new HashMap<String, String>();
    col1.addProperties(props);
    ExprColumn col1FromProps = new ExprColumn("multiExprCol", props);
    assertEquals(col1, col1FromProps);
    assertEquals(col1.hashCode(), col1FromProps.hashCode());
    assertEquals(col1FromProps.getExpressionSpecs().iterator().next().getStartTime(), twoDaysBackUptoHours);

    col1 = new ExprColumn(colSchema, "multiExprColDisplayString", new ExprSpec("avg(MSR1) + avg(MSR2)",
      null, twoDaysBack));
    props = new HashMap<String, String>();
    col1.addProperties(props);
    col1FromProps = new ExprColumn("multiExprCol", props);
    assertEquals(col1, col1FromProps);
    assertEquals(col1.hashCode(), col1FromProps.hashCode());
    assertEquals(col1FromProps.getExpressionSpecs().iterator().next().getEndTime(), twoDaysBackUptoHours);


    col1 = new ExprColumn(colSchema, "multiExprColDisplayString", new ExprSpec("avg(MSR1) + avg(MSR2)",
      twoDaysBack, now));
    props = new HashMap<String, String>();
    col1.addProperties(props);
    col1FromProps = new ExprColumn("multiExprCol", props);
    assertEquals(col1, col1FromProps);
    assertEquals(col1.hashCode(), col1FromProps.hashCode());
    assertEquals(col1FromProps.getExpressionSpecs().iterator().next().getStartTime(), twoDaysBackUptoHours);
    assertEquals(col1FromProps.getExpressionSpecs().iterator().next().getEndTime(), nowUptoHours);
  }

  @Test
  public void testMultipleExpressionStartAndEndTimes() throws Exception {
    FieldSchema colSchema = new FieldSchema("multiExprCol", "double", "multi exprcol");

    ExprColumn col1 = new ExprColumn(colSchema, "multiExprColDisplayString", new ExprSpec("avg(MSR1) + avg(MSR2)",
      twoDaysBack, null), new ExprSpec("avg(MSR1) + avg(MSR2) - m1 + m1", now, null),
      new ExprSpec("avg(MSR1) + avg(MSR2)", null, twoDaysBack),
      new ExprSpec("avg(MSR1) + avg(MSR2) + 0.01", twoDaysBack, now));
    Map<String, String> props = new HashMap<String, String>();
    col1.addProperties(props);
    ExprColumn col1FromProps = new ExprColumn("multiExprCol", props);
    assertEquals(col1, col1FromProps);
    assertEquals(col1.hashCode(), col1FromProps.hashCode());
    Iterator<ExprSpec> it = col1FromProps.getExpressionSpecs().iterator();
    assertEquals(it.next().getStartTime(), twoDaysBackUptoHours);
    assertEquals(it.next().getStartTime(), nowUptoHours);
    ExprSpec endTimeSpecified = it.next();
    assertNull(endTimeSpecified.getStartTime());
    assertEquals(endTimeSpecified.getEndTime(), twoDaysBackUptoHours);
    ExprSpec last = it.next();
    assertEquals(last.getStartTime(), twoDaysBackUptoHours);
    assertEquals(last.getEndTime(), nowUptoHours);
    assertFalse(it.hasNext());
  }

  @Test
  public void testExprColumnCreationErrors() throws LensException {
    FieldSchema colSchema = new FieldSchema("errorColumn", "double", "multi exprcol");

    // no expression spec passed
    try {
      ExprColumn col1 = new ExprColumn(colSchema, "NoExprSpec", (ExprSpec[])null);
      fail(col1 + " should not be created");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("No expressions specified for column errorColumn"));
    }

    // no expression passed in exprspec
    try {
      ExprColumn col1 = new ExprColumn(colSchema, "NoExprInExprSpec", new ExprSpec(null, null, null));
      fail(col1 + " should not be created");
    } catch (NullPointerException e) {
      // pass
    }

    // Parse error in expr passed in exprspec
    try {
      ExprColumn col1 = new ExprColumn(colSchema, "NoExprInExprSpec", new ExprSpec("(a+b", null, null));
      fail(col1 + " should not be created");
    } catch (LensException e) {
      assertEquals(e.getErrorCode(), LensCubeErrorCode.EXPRESSION_NOT_PARSABLE.getLensErrorInfo().getErrorCode());
    }

    // Parse error in expr passed in exprspec
    try {
      ExprColumn col1 = new ExprColumn(colSchema, "NoExprInExprSpec", new ExprSpec("a + b", null, null),
        new ExprSpec("(a+b", null, null));
      fail(col1 + " should not be created");
    } catch (LensException e) {
      assertEquals(e.getErrorCode(), LensCubeErrorCode.EXPRESSION_NOT_PARSABLE.getLensErrorInfo().getErrorCode());
    }

    // no expression passed in exprspec
    try {
      ExprColumn col1 = new ExprColumn(colSchema, "NoExprInExprSpecAt1", new ExprSpec("a + b", null, null),
        new ExprSpec(null, null, null));
      fail(col1 + " should not be created");
    } catch (NullPointerException e) {
      // pass
    }

    // startTime after endTime
    try {
      ExprColumn col1 = new ExprColumn(colSchema, "startTimeAfterEndTime", new ExprSpec("a + b", now, twoDaysBack));
      fail(col1 + " should not be created");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Start time is after end time for column errorColumn "
        + "for expression at index:0"), e.getMessage());
    }

    // startTime after endTime
    try {
      ExprColumn col1 = new ExprColumn(colSchema, "startTimeAfterEndTimeAt1", new ExprSpec("a + b", null, null),
        new ExprSpec("a + b", now, twoDaysBack));
      fail(col1 + " should not be created");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Start time is after end time for column errorColumn "
        + "for expression at index:1"), e.getMessage());
    }
  }
}
