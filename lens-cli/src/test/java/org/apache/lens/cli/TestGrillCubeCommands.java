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


import org.apache.lens.cli.commands.GrillCubeCommands;
import org.apache.lens.client.GrillClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;
import java.net.URL;

public class TestGrillCubeCommands extends GrillCliApplicationTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestGrillCubeCommands.class);

  @Test
  public void testCubeCommands() throws Exception {
    GrillClient client = new GrillClient();
    GrillCubeCommands command = new GrillCubeCommands();
    command.setClient(client);
    LOG.debug("Starting to test cube commands");
    URL cubeSpec =
        TestGrillCubeCommands.class.getClassLoader().getResource("sample-cube.xml");
    String cubeList = command.showCubes();
    Assert.assertFalse(
        cubeList.contains("sample_cube"));
    command.createCube(new File(cubeSpec.toURI()).getAbsolutePath());
    cubeList = command.showCubes();
    Assert.assertTrue(
        cubeList.contains("sample_cube"));

    testUpdateCommand(new File(cubeSpec.toURI()), command);
    command.dropCube("sample_cube");
    cubeList = command.showCubes();
    Assert.assertFalse(
        cubeList.contains("sample_cube"));
  }

  private void testUpdateCommand(File f, GrillCubeCommands command) {
    try {
      StringBuilder sb = new StringBuilder();
      BufferedReader bufferedReader = new BufferedReader(new FileReader(f));
      String s;
      while ((s = bufferedReader.readLine()) != null) {
        sb.append(s).append("\n");
      }

      bufferedReader.close();

      String xmlContent = sb.toString();

      xmlContent = xmlContent.replace("<properties name=\"sample_cube.prop\" value=\"sample\" />\n",
          "<properties name=\"sample_cube.prop\" value=\"sample\" />" +
              "\n<properties name=\"sample_cube.prop1\" value=\"sample1\" />\n");

      File newFile = new File("/tmp/sample_cube1.xml");
      Writer writer = new OutputStreamWriter(new FileOutputStream(newFile));
      writer.write(xmlContent);
      writer.close();

      String desc = command.describeCube("sample_cube");
      GrillClient client = command.getClient();
      LOG.debug(desc);
      String propString = "name : sample_cube.prop  value : sample";
      String propString1 = "name : sample_cube.prop1  value : sample1";

      Assert.assertTrue(desc.contains(propString));

      command.updateCube("sample_cube /tmp/sample_cube1.xml");
      desc = command.describeCube("sample_cube");
      LOG.debug(desc);
      Assert.assertTrue(desc.contains(propString));
      Assert.assertTrue(desc.contains(propString1));

      newFile.delete();

    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Testing update cube failed with exception" + t.getMessage());
    }
  }
}
