package com.inmobi.grill.server;

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

import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.ui.UIApp;
import org.glassfish.jersey.filter.LoggingFilter;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.Set;

import static org.testng.Assert.assertEquals;


@Test(groups="unit-test")
public class TestStaticFileResource extends GrillJerseyTest {

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
  }

  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
  }


  @Override
  protected int getTestPort() {
    return 19999;
  }

  @Override
  protected Application configure() {
    return new UIApp() {
      @Override
      public Set<Class<?>> getClasses() {
        Set<Class<?>> classes = super.getClasses();
        classes.add(LoggingFilter.class);
        return classes;
      }
    };
  }

  @Override
  protected URI getUri() {
    return UriBuilder.fromUri("http://localhost/").port(getTestPort()).build();
  }

  @Override
  protected URI getBaseUri() {
    return getUri();
  }

  @Test
  public void testStaticFileResource()  throws Exception {
    GrillServices.get().getHiveConf().set(GrillConfConstants.GRILL_SERVER_UI_STATIC_DIR,
    "src/main/webapp/static");
    GrillServices.get().getHiveConf().setBoolean(GrillConfConstants.GRILL_SERVER_UI_ENABLE_CACHING, false);

    System.out.println("@@@@ " + target().path("index.html").getUri());
    Response response = target().path("index.html").request().get();
    assertEquals(response.getStatus(), 200);

    response = target().path("index234.html").request().get();
    assertEquals(response.getStatus(), 404);
  }

}
