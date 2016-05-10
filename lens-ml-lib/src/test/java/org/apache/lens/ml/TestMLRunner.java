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

import java.net.URI;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.UriBuilder;

import org.apache.lens.client.LensClient;
import org.apache.lens.client.LensClientConfig;
import org.apache.lens.client.LensMLClient;
import org.apache.lens.ml.impl.MLRunner;
import org.apache.lens.ml.impl.MLTask;
import org.apache.lens.ml.server.MLApp;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.metastore.MetastoreResource;
import org.apache.lens.server.query.QueryServiceResource;
import org.apache.lens.server.session.SessionResource;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.Hive;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;

@Test
@Slf4j
public class TestMLRunner extends LensJerseyTest {
  private static final String TEST_DB = TestMLRunner.class.getSimpleName();

  private LensMLClient mlClient;

  @Override
  protected Application configure() {
    return new MLApp(SessionResource.class, QueryServiceResource.class, MetastoreResource.class);
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
    hive.dropDatabase(TEST_DB);
    mlClient.close();
  }

  @Test
  public void trainAndEval() throws Exception {
    log.info("Starting train & eval");
    String algoName = "spark_naive_bayes";
    String database = "default";
    String trainTable = "naivebayes_training_table";
    String trainFile = "data/naive_bayes/train.data";
    String testTable = "naivebayes_test_table";
    String testFile = "data/naive_bayes/test.data";
    String outputTable = "naivebayes_eval_table";
    String[] features = { "feature_1", "feature_2", "feature_3" };
    String labelColumn = "label";

    MLRunner runner = new MLRunner();
    runner.init(mlClient, algoName, database, trainTable, trainFile,
        testTable, testFile, outputTable, features, labelColumn);
    MLTask task = runner.train();
    Assert.assertEquals(task.getTaskState(), MLTask.State.SUCCESSFUL);
    String modelID = task.getModelID();
    String reportID = task.getReportID();
    Assert.assertNotNull(modelID);
    Assert.assertNotNull(reportID);
  }

  @Test
  public void trainAndEvalFromDir() throws Exception {
    log.info("Starting train & eval from Dir");
    MLRunner runner = new MLRunner();
    runner.init(mlClient, "data/naive_bayes");
    MLTask task = runner.train();
    Assert.assertEquals(task.getTaskState(), MLTask.State.SUCCESSFUL);
    String modelID = task.getModelID();
    String reportID = task.getReportID();
    Assert.assertNotNull(modelID);
    Assert.assertNotNull(reportID);
  }
}
