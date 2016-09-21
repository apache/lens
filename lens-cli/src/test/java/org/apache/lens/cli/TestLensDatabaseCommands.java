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

import static org.testng.Assert.*;

import java.io.File;
import java.net.URISyntaxException;

import org.apache.lens.cli.commands.LensCubeCommands;
import org.apache.lens.cli.commands.LensDatabaseCommands;
import org.apache.lens.client.LensClient;
import org.apache.lens.client.exceptions.LensBriefErrorException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

/**
 * The Class TestLensDatabaseCommands.
 */
public class TestLensDatabaseCommands extends LensCliApplicationTest {

  /** The Constant LOG. */
  private static final Logger LOG = LoggerFactory.getLogger(TestLensDatabaseCommands.class);

  /**
   * Test database commands.
   */
  @Test
  public void testDatabaseCommands() throws URISyntaxException {
    try (LensClient client = new LensClient()) {
      LensDatabaseCommands command = new LensDatabaseCommands();
      LensCubeCommands cubeCommand = new LensCubeCommands();
      command.setClient(client);
      cubeCommand.setClient(client);
      boolean cascade = true;
      for (int i = 0; i < 4; i++, cascade = !cascade) {
        testDrop(command, cubeCommand, cascade);
      }
    }
  }

  private void testDrop(LensDatabaseCommands command, LensCubeCommands cubeCommand, boolean cascade)
    throws URISyntaxException {
    String myDatabase = "my_db";
    assertFalse(command.showAllDatabases().contains(myDatabase));
    assertFalse(cubeCommand.showCubes().contains("sample_cube"));
    String result;
    command.createDatabase(myDatabase, false);
    assertTrue(command.showAllDatabases().contains(myDatabase));
    result = command.switchDatabase(myDatabase);
    assertEquals(result, "Successfully switched to my_db");
    if (cascade) {
      String createOutput = cubeCommand.createCube(
        new File(TestLensDatabaseCommands.class.getClassLoader().getResource("sample-cube.xml").toURI()));
      assertEquals(createOutput, "succeeded");
      assertTrue(cubeCommand.showCubes().contains("sample_cube"));
    }
    result = command.switchDatabase("default");
    assertEquals(result, "Successfully switched to default");
    assertFalse(cubeCommand.showCubes().contains("sample_cube"));
    if (cascade) {
      try {
        command.dropDatabase(myDatabase, false);
        fail("Should have failed");
      } catch(LensBriefErrorException e) {
        assertTrue(e.getIdBriefErrorTemplate().getBriefError().toPrettyString().contains("my_db is not empty"));
      }
    }
    result = command.dropDatabase(myDatabase, cascade);
    assertEquals(result, "succeeded");
    assertFalse(command.showAllDatabases().contains(myDatabase));
    assertFalse(cubeCommand.showCubes().contains("sample_cube"));
  }
}
