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
/*
 *
 */
package org.apache.lens.server.api.driver.hooks;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.query.AbstractQueryContext;

import org.apache.hadoop.conf.Configuration;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class UserBasedQueryHookTest {

  private LensDriver driver;
  private Configuration conf;
  private UserBasedQueryHook hook = new UserBasedQueryHook();
  private AbstractQueryContext ctx;

  @BeforeMethod
  public void setUp() throws Exception {
    driver = mock(LensDriver.class);
    conf = new Configuration();
    when(driver.getConf()).thenReturn(conf);
    ctx = mock(AbstractQueryContext.class);
  }

  @DataProvider
  public Object[][] provideData() {
    return new Object[][]{
      {"a,b", "b,c", "d", false}, // disallow setting both
      {"a,b", null, "c", false}, // disallowed
      {null, "a,b", "c", true}, // not disallowed
      {"a,b", null, "a", true}, // allowed
      {null, "a,b", "a", false}, // not allowed
      {null, null, "a", true}, // no restrictions
      {"", "", "a", true}, // null and blank string are same
    };
  }

  @Test(dataProvider = "provideData")
  public void testPreRewrite(String allowedString, String disallowedString, String user, boolean success)
    throws Exception {
    if (allowedString != null) {
      conf.set(UserBasedQueryHook.ALLOWED_USERS, allowedString);
    } else {
      conf.unset(UserBasedQueryHook.ALLOWED_USERS);
    }
    if (disallowedString != null) {
      conf.set(UserBasedQueryHook.DISALLOWED_USERS, disallowedString);
    } else {
      conf.unset(UserBasedQueryHook.DISALLOWED_USERS);
    }
    when(ctx.getSubmittedUser()).thenReturn(user);
    try {
      hook.setDriver(driver);
      hook.preRewrite(ctx);
      assertTrue(success);
    } catch (Exception e) {
      assertFalse(success);
    }
  }
}
