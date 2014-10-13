package org.apache.lens.cli;
/*
 * #%L
 * Grill CLI
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


import org.apache.lens.server.GrillAllApplicationJerseyTest;
import org.apache.lens.server.GrillServices;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;


public class GrillCliApplicationTest extends GrillAllApplicationJerseyTest {

  @Override
  protected int getTestPort() {
    return 9999;
  }

  @Override
  protected URI getBaseUri() {
    return UriBuilder.fromUri(getUri()).path("grillapi").build();
  }

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
  }

  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
  }


}
