package com.inmobi.grill.server.session;

/*
 * #%L
 * Grill Server
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.server.GrillJerseyTest;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.auth.FooBarAuthenticationProvider;
import com.inmobi.grill.server.metastore.MetastoreApp;
import org.apache.hive.service.Service;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.ws.rs.core.Application;
import java.util.HashMap;

@Test(groups="unit-test")
public class TestSessionService  extends GrillJerseyTest {
  private HiveSessionService sessionService;

  @BeforeTest
  public void init(){
    sessionService = GrillServices.get().getService("session");
  }
  @Test
  public void testWrongAuth() {
    try{
      sessionService.openSession("a","b", new HashMap<String, String>());
      Assert.fail("open session shouldn't have been succeeded");
    } catch (GrillException e) {
      Assert.assertEquals(e.getCause().getCause().getMessage(), FooBarAuthenticationProvider.MSG);
    }
  }

  @Test
  public void testCorrectAuth() {
    try{
      sessionService.openSession("foo","bar", new HashMap<String, String>());
    } catch (GrillException e) {
      Assert.fail("Shouldn't have thrown exception");
    }
  }

  @Override
  protected int getTestPort() {
    return 8084;
  }

  @Override
  protected Application configure() {
    return new SessionApp();
  }
}
