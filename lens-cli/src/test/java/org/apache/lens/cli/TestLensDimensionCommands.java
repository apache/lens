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

import org.apache.lens.cli.commands.LensDimensionCommands;
import org.apache.lens.client.LensClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * The Class TestLensDimensionCommands.
 */
public class TestLensDimensionCommands extends LensCliApplicationTest {

  /** The Constant LOG. */
  private static final Logger LOG = LoggerFactory.getLogger(TestLensDimensionCommands.class);

  /** The command. */
  private static LensDimensionCommands command = null;

  private static LensDimensionCommands getCommand() {
    if (command == null) {
      LensClient client = new LensClient();
      command = new LensDimensionCommands();
      command.setClient(client);
    }
    return command;
  }

  /**
   * Creates the dimension.
   *
   * @throws URISyntaxException
   *           the URI syntax exception
   */
  public static void createDimension() throws URISyntaxException {
    URL dimensionSpec = TestLensDimensionCommands.class.getClassLoader().getResource("test-dimension.xml");
    getCommand().createDimension(new File(dimensionSpec.toURI()).getAbsolutePath());
  }

  /**
   * Test dimension commands.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testDimensionCommands() throws Exception {
    LOG.debug("Starting to test dimension commands");
    URL dimensionSpec = TestLensDimensionCommands.class.getClassLoader().getResource("test-dimension.xml");
    String dimensionList = getCommand().showDimensions();
    Assert.assertFalse(dimensionList.contains("test_dim"));
    createDimension();
    dimensionList = getCommand().showDimensions();
    Assert.assertTrue(dimensionList.contains("test_dim"));

    testUpdateCommand(new File(dimensionSpec.toURI()), getCommand());
    getCommand().dropDimension("test_dim");
    dimensionList = getCommand().showDimensions();
    Assert.assertFalse(dimensionList.contains("test_dim"));
  }

  /**
   * Test update command.
   *
   * @param f
   *          the f
   * @param command
   *          the command
   */
  private void testUpdateCommand(File f, LensDimensionCommands command) {
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
          "<properties name=\"test_dim.prop\" value=\"test\" />"
              + "\n<properties name=\"test_dim.prop1\" value=\"test1\" />\n");

      File newFile = new File("/tmp/test_dim1.xml");
      Writer writer = new OutputStreamWriter(new FileOutputStream(newFile));
      writer.write(xmlContent);
      writer.close();

      String desc = command.describeDimension("test_dim");
      LOG.debug(desc);
      String propString = "name : test_dim.prop  value : test";
      String propString1 = "name : test_dim.prop1  value : test1";
      Assert.assertTrue(desc.contains(propString));

      command.updateDimension("test_dim /tmp/test_dim1.xml");
      desc = command.describeDimension("test_dim");
      LOG.debug(desc);
      Assert.assertTrue(desc.contains(propString));

      Assert.assertTrue(desc.contains(propString1));

      newFile.delete();

    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Testing update dimension failed with exception" + t.getMessage());
    }
  }
}
