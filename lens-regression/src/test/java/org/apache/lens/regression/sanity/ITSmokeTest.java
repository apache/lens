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
package org.apache.lens.regression.sanity;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;


import org.apache.lens.regression.core.testHelper.BaseTestClass;
import org.apache.lens.regression.util.Util;

import org.apache.log4j.Logger;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.jcraft.jsch.JSchException;

public class ITSmokeTest extends BaseTestClass {

  private static final Logger LOGGER = Logger.getLogger(ITSmokeTest.class);
  private final String resourceDir = "src/test/resources";
  private final String resultFile = resourceDir + "/result.data";
  private final String smokeOutput = resourceDir + "/output.txt";
  private final String clientDir = Util.getProperty("lens.client.dir");
  private final String dbName = "smoketest";

  @BeforeMethod(alwaysRun = true)
  public void setUp(Method method) throws Exception {
    LOGGER.info("Test Name: " + method.getName());
  }

  @Test(enabled = true, groups = { "integration" })
  public void smokeTest() throws Exception {

    String exampleScript = clientDir + "/bin/run-examples.sh";
    String sampleMetastoreCommand = "bash " + exampleScript + " sample-metastore -db " + dbName;
    String populateMetastoreCommand = "bash " + exampleScript + " populate-metastore -db " + dbName;
    String runQueriesCommand = "bash " + exampleScript + " runqueries -db " + dbName;

    LOGGER.info("Creating schema : ");
    String output = Util.runRemoteCommand(sampleMetastoreCommand);
    System.out.println("Output : " + output);
    LOGGER.info("Output : " + output);

    LOGGER.info("Populating Metastore : ");
    output = Util.runRemoteCommand(populateMetastoreCommand);
    LOGGER.info("Output : " + output);

    LOGGER.info("Running Queries in Background : ");
    output = Util.runRemoteCommand("nohup " + runQueriesCommand + " > smoke.log 2>&1 &");
    LOGGER.info("Output : " + output);

    waitToComplete();

    output = Util.runRemoteCommand("cat smoke.log");
    LOGGER.info("Output : " + output);

    Util.writeFile(smokeOutput, output);

    Assert.assertTrue(output.contains("Successful queries 146"), "Some Queries Failed");
    Assert.assertTrue(compareFile(smokeOutput, resultFile), "Result Validation Failed");
  }

  private void waitToComplete() throws IOException, JSchException, InterruptedException {
    boolean loop = true;
    while (loop) {
      String output = Util.runRemoteCommand("ps -ef | grep org.apache.lens.examples.SampleQueries | grep -v \"grep\" ");
      if (output.length() == 0) {
        loop = false;
      }
      Thread.sleep(60000);
    }

  }

  private boolean compareFile(String file1, String file2) throws IOException {
    String skipDriverTime = "Driver run time in millis";
    String skipTotalTime = "Total time for running examples(in millis)";
    String skipTime = "Total time in millis";
    BufferedReader reader1 = new BufferedReader(new FileReader(new File(file1)));
    BufferedReader reader2 = new BufferedReader(new FileReader(new File(file2)));
    boolean validate = true;
    try {
      String line1 = reader1.readLine();
      String line2 = reader2.readLine();
      while ((validate) && (line1 != null) && (line2 != null)) {
        if (line1.contains(skipTime) || line1.contains(skipDriverTime) || line1.contains(skipTotalTime)) {
          line1 = reader1.readLine();
          line2 = reader2.readLine();
        } else if (!line1.equalsIgnoreCase(line2)) {
          validate = false;
        } else {
          line1 = reader1.readLine();
          line2 = reader2.readLine();
        }
      }
    } finally {
      reader1.close();
      reader2.close();
    }
    return validate;
  }

}
