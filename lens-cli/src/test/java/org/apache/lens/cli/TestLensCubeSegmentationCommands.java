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

import javax.ws.rs.NotFoundException;

import org.apache.lens.cli.commands.LensCubeCommands;
import org.apache.lens.cli.commands.annotations.LensCubeSegmentationCommands;
import org.apache.lens.client.LensClient;

import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestLensCubeSegmentationCommands extends LensCliApplicationTest {

  private static LensCubeSegmentationCommands command = null;
  private static LensCubeCommands cubeCommands = null;

  private void createSampleCube() throws URISyntaxException {
    URL cubeSpec = TestLensCubeCommands.class.getClassLoader().getResource("sample-cube.xml");
    String cubeList = getCubeCommand().showCubes();
    assertFalse(cubeList.contains("sample_cube"), cubeList);
    getCubeCommand().createCube(new File(cubeSpec.toURI()));
    cubeList = getCubeCommand().showCubes();
    assertTrue(cubeList.contains("sample_cube"), cubeList);
  }

  private static LensCubeSegmentationCommands getCommand() {
    if (command == null) {
      LensClient client = new LensClient();
      command = new LensCubeSegmentationCommands();
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

  public static void testCreateSegmentation() throws IOException {
    LensCubeSegmentationCommands command = getCommand();
    String segList = command.showCubeSegmentations(null);
    assertEquals(command.showCubeSegmentations("sample_cube"), "No cubesegmentation found for sample_cube");
    assertEquals(segList, "No cubesegmentation found");
    URL segSpec = TestLensCubeSegmentationCommands.class.getClassLoader().getResource("seg1.xml");
    try {
      command.createCubeSegmentation(new File(segSpec.toURI()));
    } catch (Exception e) {
      fail("Unable to create cubesegmentation" + e.getMessage());
    }
    segList = command.showCubeSegmentations(null);
    assertEquals(command.showCubeSegmentations("sample_cube"), segList);
    try {
      assertEquals(command.showCubeSegmentations("blah"), segList);
      fail();
    } catch (NotFoundException e) {
      log.info("blah is not a cubesegmentation", e);
    }
  }

  public static void testUpdateSegmentation() {
    try {
      LensCubeSegmentationCommands command = getCommand();
      URL segSpec = TestLensCubeSegmentationCommands.class.getClassLoader().getResource("seg1.xml");
      StringBuilder sb = new StringBuilder();
      BufferedReader bufferedReader = new BufferedReader(new FileReader(segSpec.getFile()));
      String s;
      while ((s = bufferedReader.readLine()) != null) {
        sb.append(s).append("\n");
      }

      bufferedReader.close();
      String xmlContent = sb.toString();
      xmlContent = xmlContent.replace("<property name=\"seg1.prop\" value=\"s1\"/>\n",
          "<property name=\"seg1.prop\" value=\"s1\"/>" + "\n<property name=\"seg1.prop1\" value=\"s2\"/>\n");

      File newFile = new File("target/seg2.xml");
      Writer writer = new OutputStreamWriter(new FileOutputStream(newFile));
      writer.write(xmlContent);
      writer.close();

      String desc = command.describeCubeSegmentation("seg1");
      log.debug(desc);
      String propString = "seg1.prop: s1";
      String propString1 = "seg1.prop1: s2";

      assertTrue(desc.contains(propString));

      command.updateCubeSegmentation("seg1", new File("target/seg2.xml"));
      desc = command.describeCubeSegmentation("seg1");
      log.debug(desc);
      assertTrue(desc.contains(propString), "The sample property value is not set");
      assertTrue(desc.contains(propString1), "The sample property value is not set");

      newFile.delete();

    } catch (Throwable t) {
      log.error("Updating of the cubesegmentation failed with ", t);
      fail("Updating of the cubesegmentation failed with " + t.getMessage());
    }

  }

  public static void testDropSegmentation() {
    LensCubeSegmentationCommands command = getCommand();
    String segList = command.showCubeSegmentations(null);
    assertEquals("seg1", segList, "seg1 segmentation should be found");
    command.dropCubeSegmentation("seg1");
    segList = command.showCubeSegmentations(null);
    assertEquals(segList, "No cubesegmentation found");
  }

  private void dropSampleCube() {
    getCubeCommand().dropCube("sample_cube");
  }

  @Test
  public void testCubeSegmentationCommands() throws IOException, URISyntaxException {
    createSampleCube();
    testCreateSegmentation();
    testUpdateSegmentation();
    testDropSegmentation();
    dropSampleCube();
  }

}
