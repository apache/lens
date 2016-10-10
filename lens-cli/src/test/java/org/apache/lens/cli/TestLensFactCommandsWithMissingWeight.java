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
package org.apache.lens.cli;

import java.io.*;

import java.net.URISyntaxException;
import java.net.URL;

import org.apache.lens.cli.commands.LensCubeCommands;
import org.apache.lens.cli.commands.LensFactCommands;
import org.apache.lens.client.LensClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;


/**
 * The Class TestLensFactCommands.
 */
public class TestLensFactCommandsWithMissingWeight extends LensCliApplicationTest {

  /** The Constant LOG. */
  private static final Logger LOG = LoggerFactory.getLogger(TestLensFactCommandsWithMissingWeight.class);

  /** The Constant FACT_LOCAL. */
  public static final String FACT_LOCAL = "fact_local_without_wt";

  /* The Constant for cube name */
  public static final String CUBE_NAME = "cube_with_no_weight_facts";

  /* The Constant for fact name */
  public static final String FACT_NAME = "fact_without_wt";

  /* The File name with cube details */
  public static final String CUBE_XML_FILE = "schema/cubes/base/cube_with_no_weight_facts.xml";

  /* The File name with fact details */
  public static final String FACT_XML_FILE = "schema/facts/fact_without_weight.xml";

  /** The command. */
  private static LensFactCommands command = null;
  private static LensCubeCommands cubeCommands = null;

  /**
   * Test fact commands.
   *
   * @throws java.io.IOException
   */
  @Test
  public void testFactCommands() throws IOException, URISyntaxException {
    dropSampleCubeIfExists();
    dropFactIfExists();

    createSampleCube();
    addFactTable();
    dropSampleCube();
  }

  private void createSampleCube() throws URISyntaxException {
    URL cubeSpec = TestLensCubeCommands.class.getClassLoader().getResource(CUBE_XML_FILE);
    String cubeList = getCubeCommand().showCubes();
    Assert.assertFalse(cubeList.contains(CUBE_NAME));
    getCubeCommand().createCube(new File(cubeSpec.toURI()));
    cubeList = getCubeCommand().showCubes();
    Assert.assertTrue(cubeList.contains(CUBE_NAME));
  }

  private void dropSampleCube() {
    getCubeCommand().dropCube(CUBE_NAME);
    TestLensStorageCommands.dropStorage(FACT_LOCAL);
  }

  private static LensFactCommands getCommand() {
    if (command == null) {
      LensClient client = new LensClient();
      command = new LensFactCommands();
      command.setClient(client);
    }
    return command;
  }

  private static LensCubeCommands getCubeCommand() {
    if (cubeCommands == null) {
      LensClient client = new LensClient();
      cubeCommands = new LensCubeCommands();
      cubeCommands.setClient(client);
    }
    return cubeCommands;
  }

  @AfterTest
  public void cleanUp() {
    if (command != null) {
      command.getClient().closeConnection();
    }
    if (cubeCommands != null) {
      cubeCommands.getClient().closeConnection();
    }
  }

  /**
   * Adds the fact_without_wt table.
   *
   * @throws java.io.IOException
   */
  public static void addFactTable() throws IOException, URISyntaxException {
    LensFactCommands command = getCommand();
    String factList = command.showFacts(null);
    Assert.assertEquals(command.showFacts(CUBE_NAME), "No fact found for " + CUBE_NAME);
    Assert.assertEquals(factList, "No fact found", "Fact tables should not be found.");
    // add local storage before adding fact table
    TestLensStorageCommands.addLocalStorage(FACT_LOCAL);
    URL factSpec = TestLensFactCommandsWithMissingWeight.class.getClassLoader().getResource(FACT_XML_FILE);
    String response = null;
    response = command.createFact(new File(factSpec.toURI()));

    Assert.assertEquals(response, "failed", "Fact table creation should not be successful.");
    Assert.assertEquals(command.showFacts(CUBE_NAME), "No fact found for " + CUBE_NAME,
            "Fact tables should not be found.");
  }

  /**
   * Drop fact_without_wt table if exixsts.
   */
  public static void dropFactIfExists() {
    LensFactCommands command = getCommand();
    String factList = command.showFacts(null);
    if (factList.contains(FACT_NAME)) {
      command.dropFact(FACT_NAME, false);
      TestLensStorageCommands.dropStorage(FACT_LOCAL);
    }
  }

  private void dropSampleCubeIfExists() {
    String cubeList = getCubeCommand().showCubes();
    if (cubeList.contains(CUBE_NAME)) {
      getCubeCommand().dropCube(CUBE_NAME);
    }
  }
}
