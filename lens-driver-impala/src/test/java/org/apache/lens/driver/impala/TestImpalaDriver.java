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

import static org.powermock.api.mockito.PowerMockito.*;
import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.lens.driver.impala.ImpalaDriver;
import org.apache.lens.driver.impala.ImpalaResultSet;
import org.apache.lens.server.api.driver.LensResultSet;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;
import com.cloudera.beeswax.api.Query;
import com.cloudera.beeswax.api.QueryHandle;
import com.cloudera.beeswax.api.QueryState;
import com.cloudera.impala.thrift.ImpalaService;

/**
 * The Class TestImpalaDriver.
 */
@PowerMockIgnore({ "org.apache.commons.logging.*", "org.xml.*", "javax.xml.*", "org.w3c.*" })
@PrepareForTest(ImpalaDriver.class)
public class TestImpalaDriver {

  /** The test inst. */
  private ImpalaDriver testInst;

  @ObjectFactory
  public IObjectFactory getObjectFactory() {
    return new org.powermock.modules.testng.PowerMockObjectFactory();
  }

  /**
   * Public setup test.
   */
  @BeforeTest
  public void publicSetupTest() {
    testInst = new ImpalaDriver();
  }

  /**
   * Test configure.
   */
  @Test
  public void testConfigure() {

    try {
      Configuration config = new Configuration();
      config.set("PORT", "123");
      config.set("HOST", "test.com");

      TSocket mockSocket = PowerMockito.mock(TSocket.class);
      TBinaryProtocol mockTProtocol = mock(TBinaryProtocol.class);
      ImpalaService.Client mockClient = mock(ImpalaService.Client.class);

      whenNew(TSocket.class).withArguments(config.get("HOST"), config.getInt("PORT", 9999)).thenReturn(mockSocket);
      whenNew(TBinaryProtocol.class).withArguments(mockSocket).thenReturn(mockTProtocol);
      whenNew(ImpalaService.Client.class).withArguments(mockTProtocol).thenReturn(mockClient);

      this.testInst.configure(config);
      verifyNew(TSocket.class).withArguments("test.com", 123);
      verifyNew(TBinaryProtocol.class).withArguments(mockSocket);
      verifyNew(ImpalaService.Client.class).withArguments(mockTProtocol);

      Mockito.verify(mockSocket, Mockito.times(1)).open();
    } catch (Exception e) {
      Assert.fail();
    }

  }

  /**
   * Test execute.
   */
  @Test
  public void testExecute() {
    try {

      // configure before executing
      Configuration config = new Configuration();
      config.set("PORT", "123");
      config.set("HOST", "test.com");

      TSocket mockSocket = PowerMockito.mock(TSocket.class);

      TBinaryProtocol mockTProtocol = PowerMockito.mock(TBinaryProtocol.class);
      ImpalaService.Client mockClient = Mockito.mock(ImpalaService.Client.class);

      Query q = mock(Query.class);
      QueryHandle qh = mock(QueryHandle.class);
      ImpalaResultSet mockResultSet = mock(ImpalaResultSet.class);

      when(mockResultSet.hasNext()).thenReturn(true);
      whenNew(Query.class).withNoArguments().thenReturn(q);
      when(mockClient.query(q)).thenReturn(qh);
      when(mockClient.get_state(qh)).thenReturn(QueryState.FINISHED);

      whenNew(TSocket.class).withArguments(config.get("HOST"), config.getInt("PORT", 9999)).thenReturn(mockSocket);
      whenNew(TBinaryProtocol.class).withArguments(mockSocket).thenReturn(mockTProtocol);
      whenNew(ImpalaService.Client.class).withArguments(mockTProtocol).thenReturn(mockClient);
      whenNew(ImpalaResultSet.class).withArguments(mockClient, qh).thenReturn(mockResultSet);

      // actual run
      this.testInst.configure(config);
      LensResultSet br = this.testInst.execute("query", null);

      // test and verify
      Assert.assertEquals(true, ((ImpalaResultSet) br).hasNext());
      Mockito.verify(mockClient).query(q);
      Mockito.verify(mockClient).get_state(qh);

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }

  }

  /**
   * Test explain.
   */
  @Test
  public void testExplain() {

    /*
     * QueryCost qs = this.testInst.explain("query"); Assert.assertEquals(ExecMode.INTERACTIVE, qs.getExecMode());
     * Assert.assertEquals(-1, qs.getScanSize()); Assert.assertEquals(ScanMode.FULL_SCAN, qs.getScanMode());
     */
  }
}
