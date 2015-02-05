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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import org.testng.annotations.Test;

public class TestExprColumn {
  @Test
  public void testExprColumnEquality() throws Exception {
    FieldSchema colSchema = new FieldSchema("someExprCol", "double", "some exprcol");

    ExprColumn col1 = new ExprColumn(colSchema, "someExprDisplayString", "AVG(msr1) + AVG(msr2)");
    ExprColumn col2 = new ExprColumn(colSchema, "someExprDisplayString", "avg(MSR1) + avg(MSR2)");
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
}
