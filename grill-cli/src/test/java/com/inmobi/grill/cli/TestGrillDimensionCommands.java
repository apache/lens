package com.inmobi.grill.cli;
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

import com.inmobi.grill.cli.commands.GrillDimensionCommands;
import com.inmobi.grill.cli.commands.GrillDimensionTableCommands;
import com.inmobi.grill.client.GrillClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;

public class TestGrillDimensionCommands extends GrillCliApplicationTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestGrillDimensionCommands.class);

  private static GrillDimensionCommands command = null;


  private static GrillDimensionCommands getCommand() {
    if (command == null) {
      GrillClient client = new GrillClient();
      command = new GrillDimensionCommands();
      command.setClient(client);
    }
    return command;
  }

  public static void createDimension() throws URISyntaxException {
    URL dimensionSpec =
        TestGrillDimensionCommands.class.getClassLoader().getResource("test-dimension.xml");
    getCommand().createDimension(new File(dimensionSpec.toURI()).getAbsolutePath());
  }

  @Test
  public void testDimensionCommands() throws Exception {
    LOG.debug("Starting to test dimension commands");
    URL dimensionSpec =
        TestGrillDimensionCommands.class.getClassLoader().getResource("test-dimension.xml");
    String dimensionList = getCommand().showDimensions();
    Assert.assertFalse(
        dimensionList.contains("test_dim"));
    createDimension();
    dimensionList = getCommand().showDimensions();
    Assert.assertTrue(
        dimensionList.contains("test_dim"));

    testUpdateCommand(new File(dimensionSpec.toURI()), getCommand());
    getCommand().dropDimension("test_dim");
    dimensionList = getCommand().showDimensions();
    Assert.assertFalse(
        dimensionList.contains("test_dim"));
  }

  private void testUpdateCommand(File f, GrillDimensionCommands command) {
    try {
      StringBuilder sb = new StringBuilder();
      BufferedReader bufferedReader = new BufferedReader(new FileReader(f));
      String s;
      while ((s = bufferedReader.readLine()) != null) {
        sb.append(s).append("\n");
      }

      bufferedReader.close();

      String xmlContent = sb.toString();

      xmlContent = xmlContent.replace("<properties name=\"test_dim.prop\" value=\"test\" />\n",
          "<properties name=\"test_dim.prop\" value=\"test\" />" +
              "\n<properties name=\"test_dim.prop1\" value=\"test1\" />\n");

      File newFile = new File("/tmp/test_dim1.xml");
      Writer writer = new OutputStreamWriter(new FileOutputStream(newFile));
      writer.write(xmlContent);
      writer.close();

      String desc = command.describeDimension("test_dim");
      LOG.debug(desc);
      Assert.assertTrue(
          desc.contains("test_dim.prop=test"));

      command.updateDimension("test_dim /tmp/test_dim1.xml");
      desc = command.describeDimension("test_dim");
      LOG.debug(desc);
      Assert.assertTrue(
          desc.contains("test_dim.prop=test"));

      Assert.assertTrue(
          desc.contains("test_dim.prop1=test1"));

      newFile.delete();

    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Testing update dimension failed with exception" + t.getMessage());
    }
  }
}
