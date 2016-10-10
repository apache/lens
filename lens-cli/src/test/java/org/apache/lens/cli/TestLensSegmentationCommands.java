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
import org.apache.lens.cli.commands.annotations.LensSegmentationCommands;
import org.apache.lens.client.LensClient;

import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestLensSegmentationCommands extends LensCliApplicationTest {

  private static LensSegmentationCommands command = null;
  private static LensCubeCommands cubeCommands = null;

  private void createSampleCube() throws URISyntaxException {
    URL cubeSpec = TestLensCubeCommands.class.getClassLoader().getResource("schema/cubes/base/sample-cube.xml");
    String cubeList = getCubeCommand().showCubes();
    assertFalse(cubeList.contains("sample_cube"), cubeList);
    getCubeCommand().createCube(new File(cubeSpec.toURI()));
    cubeList = getCubeCommand().showCubes();
    assertTrue(cubeList.contains("sample_cube"), cubeList);
  }

  private static LensSegmentationCommands getCommand() {
    if (command == null) {
      LensClient client = new LensClient();
      command = new LensSegmentationCommands();
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
    LensSegmentationCommands command = getCommand();
    String segList = command.showSegmentations(null);
    assertEquals(command.showSegmentations("sample_cube"), "No segmentation found for sample_cube");
    assertEquals(segList, "No segmentation found");
    URL segSpec = TestLensSegmentationCommands.class.getClassLoader().getResource("schema/segmentations/seg1.xml");
    try {
      command.createSegmentation(new File(segSpec.toURI()));
    } catch (Exception e) {
      fail("Unable to create segmentation" + e.getMessage());
    }
    segList = command.showSegmentations(null);
    assertEquals(command.showSegmentations("sample_cube"), segList);
    try {
      assertEquals(command.showSegmentations("blah"), segList);
      fail();
    } catch (NotFoundException e) {
      log.info("blah is not a segmentation", e);
    }
  }

  public static void testUpdateSegmentation() {
    try {
      LensSegmentationCommands command = getCommand();
      URL segSpec = TestLensSegmentationCommands.class.getClassLoader().getResource("schema/segmentations/seg1.xml");
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

      String desc = command.describeSegmentation("seg1");
      log.debug(desc);
      String propString = "seg1.prop: s1";
      String propString1 = "seg1.prop1: s2";

      assertTrue(desc.contains(propString));

      command.updateSegmentation("seg1", new File("target/seg2.xml"));
      desc = command.describeSegmentation("seg1");
      log.debug(desc);
      assertTrue(desc.contains(propString), "The sample property value is not set");
      assertTrue(desc.contains(propString1), "The sample property value is not set");

      newFile.delete();

    } catch (Throwable t) {
      log.error("Updating of the segmentation failed with ", t);
      fail("Updating of the segmentation failed with " + t.getMessage());
    }

  }

  public static void testDropSegmentation() {
    LensSegmentationCommands command = getCommand();
    String segList = command.showSegmentations(null);
    assertEquals("seg1", segList, "seg1 segmentation should be found");
    command.dropSegmentation("seg1");
    segList = command.showSegmentations(null);
    assertEquals(segList, "No segmentation found");
  }

  private void dropSampleCube() {
    getCubeCommand().dropCube("sample_cube");
  }

  @Test
  public void testSegmentationCommands() throws IOException, URISyntaxException {
    createSampleCube();
    testCreateSegmentation();
    testUpdateSegmentation();
    testDropSegmentation();
    dropSampleCube();
  }

}
