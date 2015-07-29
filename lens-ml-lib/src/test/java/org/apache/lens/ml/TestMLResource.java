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
package org.apache.lens.ml;

public class TestMLResource {
  /*
  private static final Log LOG = LogFactory.getLog(TestMLResource.class);
  private static final String TEST_DB = "default";

  private WebTarget mlTarget;
  private LensMLClient mlClient;

  @Override
  protected int getTestPort() {
    return 10003;
  }

  @Override
  protected Application configure() {
    return new MLApp(SessionResource.class, QueryServiceResource.class);
  }

  @Override
  protected void configureClient(ClientConfig config) {
    config.register(MultiPartFeature.class);
  }

  @Override
  protected URI getBaseUri() {
    return UriBuilder.fromUri("http://localhost/").port(getTestPort()).path("/lensapi").build();
  }

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
    Hive hive = Hive.get(new HiveConf());
    Database db = new Database();
    db.setName(TEST_DB);
    hive.createDatabase(db, true);
    LensClientConfig lensClientConfig = new LensClientConfig();
    lensClientConfig.setLensDatabase(TEST_DB);
    lensClientConfig.set(LensConfConstants.SERVER_BASE_URL,
      "http://localhost:" + getTestPort() + "/lensapi");
    LensClient client = new LensClient(lensClientConfig);
    mlClient = new LensMLClient(client);
  }

  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
    Hive hive = Hive.get(new HiveConf());

    try {
      hive.dropDatabase(TEST_DB);
    } catch (Exception exc) {
      // Ignore drop db exception
      ////LOG.error(exc.getMessage());
    }
    mlClient.close();
  }

  @BeforeMethod
  public void setMLTarget() {
    mlTarget = target().path("ml");
  }

  @Test
  public void testMLResourceUp() throws Exception {
    String mlUpMsg = mlTarget.request().get(String.class);
    Assert.assertEquals(mlUpMsg, MLServiceResource.ML_UP_MESSAGE);
  }

  @Test
  public void testGetAlgos() throws Exception {
    mlClient.test();
  }
*/
}
