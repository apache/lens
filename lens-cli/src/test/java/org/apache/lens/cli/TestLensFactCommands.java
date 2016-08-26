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

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

import javax.ws.rs.NotFoundException;

import org.apache.lens.cli.commands.LensCubeCommands;
import org.apache.lens.cli.commands.LensFactCommands;
import org.apache.lens.client.LensClient;

import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;

/**
 * The Class TestLensFactCommands.
 */
@Slf4j
public class TestLensFactCommands extends LensCliApplicationTest {

  /** The Constant FACT_LOCAL. */
  public static final String FACT_LOCAL = "fact_local";

  /** The command. */
  private static LensFactCommands command = null;
  private static LensCubeCommands cubeCommands = null;

  /**
   * Test fact commands.
   *
   * @throws IOException
   */
  @Test
  public void testFactCommands() throws IOException, URISyntaxException {
    createSampleCube();
    addFact1Table();
    updateFact1Table();
    testFactStorageActions();
    testFactPartitionActions();
    dropFact1Table();
    dropSampleCube();
  }

  private void createSampleCube() throws URISyntaxException {
    URL cubeSpec = TestLensCubeCommands.class.getClassLoader().getResource("sample-cube.xml");
    String cubeList = getCubeCommand().showCubes();
    assertFalse(cubeList.contains("sample_cube"), cubeList);
    getCubeCommand().createCube(new File(cubeSpec.toURI()));
    cubeList = getCubeCommand().showCubes();
    assertTrue(cubeList.contains("sample_cube"), cubeList);
  }

  private void dropSampleCube() {
    getCubeCommand().dropCube("sample_cube");
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
   * Adds the fact1 table.
   *
   * @throws IOException
   */
  public static void addFact1Table() throws IOException {
    LensFactCommands command = getCommand();
    String factList = command.showFacts(null);
    assertEquals(command.showFacts("sample_cube"), "No fact found for sample_cube");
    assertEquals(factList, "No fact found", "Fact tables should not be found");
    // add local storage before adding fact table
    TestLensStorageCommands.addLocalStorage(FACT_LOCAL);
    URL factSpec = TestLensFactCommands.class.getClassLoader().getResource("fact1.xml");
    try {
      command.createFact(new File(factSpec.toURI()));
    } catch (Exception e) {
      fail("Unable to create fact table" + e.getMessage());
    }
    factList = command.showFacts(null);
    assertEquals(command.showFacts("sample_cube"), factList);
    try {
      assertEquals(command.showFacts("blah"), factList);
      fail();
    } catch (NotFoundException e) {
      log.info("blah is not a table", e);
    }
    try {
      assertEquals(command.showFacts("fact1"), factList);
      fail();
    } catch (NotFoundException e) {
      log.info("fact1 is a table, but not a cube table", e);
    }
    assertEquals("fact1", factList, "Fact1 table should be found");
  }

  /**
   * Update fact1 table.
   */
  public static void updateFact1Table() {
    try {
      LensFactCommands command = getCommand();
      URL factSpec = TestLensFactCommands.class.getClassLoader().getResource("fact1.xml");
      StringBuilder sb = new StringBuilder();
      BufferedReader bufferedReader = new BufferedReader(new FileReader(factSpec.getFile()));
      String s;
      while ((s = bufferedReader.readLine()) != null) {
        sb.append(s).append("\n");
      }

      bufferedReader.close();

      String xmlContent = sb.toString();

      xmlContent = xmlContent.replace("<property name=\"fact1.prop\" value=\"f1\"/>\n",
        "<property name=\"fact1.prop\" value=\"f1\"/>" + "\n<property name=\"fact1.prop1\" value=\"f2\"/>\n");

      xmlContent = xmlContent.replace("<column comment=\"\" name=\"measure3\" _type=\"FLOAT\"/>",
          "<column comment=\"\" name=\"measure3\" _type=\"FLOAT\"/>"
              + "\n<column comment=\"\" name=\"measure4\" _type=\"FLOAT\" start_time=\"2015-01-01\"/>\n");

      File newFile = new File("target/local-fact1.xml");
      Writer writer = new OutputStreamWriter(new FileOutputStream(newFile));
      writer.write(xmlContent);
      writer.close();

      String desc = command.describeFactTable("fact1");
      log.debug(desc);
      String propString = "fact1.prop: f1";
      String propString1 = "fact1.prop1: f2";
      String propStringColStartTime = "cube.fact.col.start.time.measure4: 2015-01-01";
      assertTrue(desc.contains(propString));

      command.updateFactTable("fact1", new File("target/local-fact1.xml"));
      desc = command.describeFactTable("fact1");
      log.debug(desc);
      assertTrue(desc.contains(propString), "The sample property value is not set");
      assertTrue(desc.contains(propString1), "The sample property value is not set");
      assertTrue(desc.contains(propStringColStartTime), "The sample property value is not set");

      newFile.delete();

    } catch (Throwable t) {
      log.error("Updating of the fact1 table failed with ", t);
      fail("Updating of the fact1 table failed with " + t.getMessage());
    }

  }

  /**
   * Test fact storage actions.
   */
  private static void testFactStorageActions() {
    LensFactCommands command = getCommand();
    String result = command.getFactStorages("fact1");
    assertEquals(FACT_LOCAL, result);
    command.dropAllFactStorages("fact1");
    result = command.getFactStorages("fact1");
    assertEquals(result, "No storage found for fact1");
    addLocalStorageToFact1();
    result = command.getFactStorages("fact1");
    assertNotEquals(result, "No storage found for fact1");
    command.dropStorageFromFact("fact1", FACT_LOCAL);
    result = command.getFactStorages("fact1");
    assertEquals(result, "No storage found for fact1");
    addLocalStorageToFact1();
  }

  /**
   * Adds the local storage to fact1.
   */
  private static void addLocalStorageToFact1() {
    LensFactCommands command = getCommand();
    String result;
    URL resource = TestLensFactCommands.class.getClassLoader().getResource("fact-local-storage-element.xml");
    try {
      command.addNewFactStorage("fact1", new File(resource.toURI()));
    } catch (Throwable t) {
      log.error("Unable to locate the storage part file for adding new storage to fact table fact1", t);
      fail("Unable to locate the storage part file for adding new storage to fact table fact1");
    }
    result = command.getFactStorages("fact1");
    assertEquals(FACT_LOCAL, result);

    result = command.getStorageFromFact("fact1", FACT_LOCAL);
    assertTrue(result.contains("HOURLY"));
    assertTrue(result.contains("DAILY"));

  }

  /**
   * Test fact partition actions.
   */
  private void testFactPartitionActions() throws URISyntaxException {
    LensFactCommands command = getCommand();
    verifyEmptyTimelines();
    assertTrue(command.getAllPartitionsOfFact("fact1", FACT_LOCAL, null).trim().isEmpty());
    String singlePartPath = new File(
      TestLensFactCommands.class.getClassLoader().getResource("fact1-local-part.xml").toURI()).getAbsolutePath();
    String multiplePartsPath = new File(
      TestLensFactCommands.class.getClassLoader().getResource("fact1-local-parts.xml").toURI()).getAbsolutePath();
    assertEquals(command.addPartitionToFact("fact1", FACT_LOCAL, new File(singlePartPath)), SUCCESS_MESSAGE);
    assertEquals(command.updatePartitionOfFact("fact1", FACT_LOCAL, new File(singlePartPath)), SUCCESS_MESSAGE);
    verifyAndDeletePartitions();
    assertEquals(command.addPartitionsToFact("fact1", FACT_LOCAL, new File(multiplePartsPath)), SUCCESS_MESSAGE);
    assertEquals(command.updatePartitionsOfFact("fact1", FACT_LOCAL, multiplePartsPath), SUCCESS_MESSAGE);
    verifyAndDeletePartitions();

    // Wrong files:
    try {
      command.addPartitionToFact("fact1", FACT_LOCAL, new File(multiplePartsPath));
      fail("Should fail");
    } catch (Throwable t) {
      // pass
    }
    try {
      command.updatePartitionOfFact("fact1", FACT_LOCAL, new File(multiplePartsPath));
      fail("Should fail");
    } catch (Throwable t) {
      // pass
    }

    try {
      command.addPartitionsToFact("fact1", FACT_LOCAL, new File(singlePartPath));
      fail("Should fail");
    } catch (Throwable t) {
      // pass
    }

    try {
      command.updatePartitionsOfFact("fact1", FACT_LOCAL, singlePartPath);
      fail("Should fail");
    } catch (Throwable t) {
      // pass
    }
  }

  private void verifyEmptyTimelines() {
    List<String> timelines = command.getTimelines("fact1", null, null, null);
    assertEquals(timelines.size(), 2);
    for (String timeline : timelines) {
      assertTrue(timeline.contains("EndsAndHolesPartitionTimeline"));
      assertTrue(timeline.contains("first=null"));
      assertTrue(timeline.contains("latest=null"));
      assertTrue(timeline.contains("holes=[]"));
    }
  }

  private void verifyAndDeletePartitions() {
    List<String> timelines;
    assertEquals(getCubeCommand().getLatest("sample_cube", "dt"), "2014-03-27T12:00:00:000");
    String result = command.getAllPartitionsOfFact("fact1", FACT_LOCAL, null);
    assertTrue(result.contains("HOURLY"));
    timelines = command.getTimelines("fact1", null, null, null);
    assertEquals(timelines.size(), 2);
    for (String timeline : timelines) {
      assertTrue(timeline.contains("EndsAndHolesPartitionTimeline"));
      if (timeline.contains("DAILY")) {
        assertTrue(timeline.contains("first=null"));
        assertTrue(timeline.contains("latest=null"));
        assertTrue(timeline.contains("holes=[]"));
      } else {
        assertTrue(timeline.contains("first=2014-03-27-12"));
        assertTrue(timeline.contains("latest=2014-03-27-12"));
        assertTrue(timeline.contains("holes=[]"));
      }
    }
    assertEquals(command.getTimelines("fact1", FACT_LOCAL, null, null), timelines);
    assertTrue(timelines.containsAll(command.getTimelines("fact1", FACT_LOCAL, "hourly", null)));
    assertTrue(timelines.containsAll(command.getTimelines("fact1", FACT_LOCAL, "hourly", "dt")));
    assertEquals(command.getTimelines("fact1", null, null, "dt"), timelines);
    assertEquals(command.getTimelines("fact1", FACT_LOCAL, null, "dt"), timelines);
    String dropPartitionsStatus = command.dropAllPartitionsOfFact("fact1", FACT_LOCAL, null);
    assertFalse(dropPartitionsStatus.contains("Syntax error, please try in following"));
    result = command.getAllPartitionsOfFact("fact1", FACT_LOCAL, null);
    assertTrue(result.trim().isEmpty());
  }

  /**
   * Drop fact1 table.
   */
  public static void dropFact1Table() {
    LensFactCommands command = getCommand();
    String factList = command.showFacts(null);
    assertEquals("fact1", factList, "Fact1 table should be found");
    command.dropFact("fact1", false);
    factList = command.showFacts(null);
    assertEquals(factList, "No fact found", "Fact tables should not be found");
    TestLensStorageCommands.dropStorage(FACT_LOCAL);
  }
}
