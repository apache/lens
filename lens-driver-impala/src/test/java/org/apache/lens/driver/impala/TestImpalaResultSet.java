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
package org.apache.lens.driver.impala;

import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.ArrayList;
import java.util.List;

import static org.powermock.api.mockito.PowerMockito.*;

import org.apache.lens.driver.impala.ImpalaResultSet;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.Assert;
import org.testng.IObjectFactory;

import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import com.cloudera.beeswax.api.QueryHandle;
import com.cloudera.beeswax.api.Results;
import com.cloudera.impala.thrift.ImpalaService;

/**
 * The Class TestImpalaResultSet.
 */
@PrepareForTest(ImpalaResultSet.class)
public class TestImpalaResultSet {

  @ObjectFactory
  public IObjectFactory getObjectFactory() {
    return new org.powermock.modules.testng.PowerMockObjectFactory();
  }

  /**
   * Test size.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testSize() throws Exception {
    List<String> returnResultSet = new ArrayList<String>();
    returnResultSet.add("one		two");
    ImpalaService.Client mockClient = mock(ImpalaService.Client.class);
    Results resultSet = mock(Results.class);
    resultSet.setData(returnResultSet);
    QueryHandle qh = mock(QueryHandle.class);
    when(mockClient.fetch(qh, false, -1)).thenReturn(resultSet);
    when(resultSet.getData()).thenReturn(returnResultSet);
    when(resultSet.isHas_more()).thenReturn(false);

    ImpalaResultSet is = new ImpalaResultSet(mockClient, qh);

    List<Object> result = is.next().getValues();

    // test and verify
    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0), "one");
    Assert.assertEquals(result.get(1), "two");

    Mockito.verify(resultSet, Mockito.times(1)).getData();
    Mockito.verify(mockClient, Mockito.times(1)).fetch(qh, false, -1);
    Mockito.verify(mockClient, Mockito.times(1)).close(qh);

    // check the size
    Assert.assertEquals(is.size(), 1);

  }

  /**
   * Checks for next.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void hasNext() throws Exception {
    List<String> returnResultSet = new ArrayList<String>();
    returnResultSet.add("one		two");
    returnResultSet.add("three		four");
    ImpalaService.Client mockClient = mock(ImpalaService.Client.class);
    Results resultSet = mock(Results.class);
    resultSet.setData(returnResultSet);
    QueryHandle qh = mock(QueryHandle.class);
    when(mockClient.fetch(qh, false, -1)).thenReturn(resultSet);
    when(resultSet.getData()).thenReturn(returnResultSet);
    when(resultSet.isHas_more()).thenReturn(false);

    ImpalaResultSet is = new ImpalaResultSet(mockClient, qh);

    Assert.assertEquals(is.hasNext(), true);

    // do first next
    List<Object> result = is.next().getValues();

    // test and verify
    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0), "one");
    Assert.assertEquals(result.get(1), "two");

    Mockito.verify(resultSet, Mockito.times(1)).getData();
    Mockito.verify(mockClient, Mockito.times(1)).fetch(qh, false, -1);
    Mockito.verify(mockClient, Mockito.times(1)).close(qh);

    // check the size
    Assert.assertEquals(is.hasNext(), true);

    // do second next to empty the result set
    result = is.next().getValues();

    // test and verify
    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0), "three");
    Assert.assertEquals(result.get(1), "four");

    Mockito.verify(resultSet, Mockito.times(1)).getData();
    Mockito.verify(mockClient, Mockito.times(1)).fetch(qh, false, -1);
    Mockito.verify(mockClient, Mockito.times(1)).close(qh);

    // check the size
    Assert.assertEquals(is.hasNext(), false);

  }

  /**
   * Next without more.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void nextWithoutMore() throws Exception {

    List<String> returnResultSet = new ArrayList<String>();
    returnResultSet.add("one		two");
    ImpalaService.Client mockClient = mock(ImpalaService.Client.class);
    Results resultSet = mock(Results.class);
    resultSet.setData(returnResultSet);
    QueryHandle qh = mock(QueryHandle.class);
    when(mockClient.fetch(qh, false, -1)).thenReturn(resultSet);
    when(resultSet.getData()).thenReturn(returnResultSet);
    when(resultSet.isHas_more()).thenReturn(false);

    ImpalaResultSet is = new ImpalaResultSet(mockClient, qh);

    List<Object> result = is.next().getValues();

    // test and verify
    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0), "one");
    Assert.assertEquals(result.get(1), "two");

    Mockito.verify(resultSet, Mockito.times(1)).getData();
    Mockito.verify(mockClient, Mockito.times(1)).fetch(qh, false, -1);
    Mockito.verify(mockClient, Mockito.times(1)).close(qh);

  }

  /**
   * Next with more.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void nextWithMore() throws Exception {

    List<String> returnResultSet = new ArrayList<String>();
    returnResultSet.add("one		two");
    ImpalaService.Client mockClient = mock(ImpalaService.Client.class);
    Results resultSet = mock(Results.class);
    resultSet.setData(returnResultSet);
    QueryHandle qh = mock(QueryHandle.class);
    when(mockClient.fetch(qh, false, -1)).thenReturn(resultSet);
    when(resultSet.getData()).thenReturn(returnResultSet);
    when(resultSet.isHas_more()).thenReturn(true);

    ImpalaResultSet is = new ImpalaResultSet(mockClient, qh);

    List<Object> result = is.next().getValues();

    // test and verify
    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0), "one");
    Assert.assertEquals(result.get(1), "two");

    Mockito.verify(resultSet, Mockito.times(1)).getData();
    Mockito.verify(mockClient, Mockito.times(1)).fetch(qh, false, -1);
    Mockito.verify(mockClient, Mockito.times(0)).close(qh);

  }

  /**
   * Next with two calls.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void nextWithTwoCalls() throws Exception {

    List<String> returnResultSet = new ArrayList<String>();
    returnResultSet.add("one		two");
    ImpalaService.Client mockClient = mock(ImpalaService.Client.class);
    Results resultSet = mock(Results.class);
    resultSet.setData(returnResultSet);
    QueryHandle qh = mock(QueryHandle.class);
    when(mockClient.fetch(qh, false, -1)).thenReturn(resultSet);
    when(resultSet.getData()).thenReturn(returnResultSet);
    when(resultSet.isHas_more()).thenReturn(true);

    ImpalaResultSet is = new ImpalaResultSet(mockClient, qh);

    List<Object> result = is.next().getValues();

    // test and verify
    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0), "one");
    Assert.assertEquals(result.get(1), "two");

    Mockito.verify(resultSet, Mockito.times(1)).getData();
    Mockito.verify(mockClient, Mockito.times(1)).fetch(qh, false, -1);
    Mockito.verify(mockClient, Mockito.times(0)).close(qh);
    when(resultSet.isHas_more()).thenReturn(false);
    result = is.next().getValues();

    Mockito.verify(resultSet, Mockito.times(2)).getData();
    Mockito.verify(mockClient, Mockito.times(2)).fetch(qh, false, -1);
    Mockito.verify(mockClient, Mockito.times(1)).close(qh);

  }
}
