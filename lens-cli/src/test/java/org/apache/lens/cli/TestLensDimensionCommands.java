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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;

import org.apache.lens.api.metastore.XJoinChains;
import org.apache.lens.cli.commands.LensDimensionCommands;
import org.apache.lens.cli.table.XJoinChainTable;
import org.apache.lens.client.LensClient;

import org.testng.Assert;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;

/**
 * The Class TestLensDimensionCommands.
 */
@Slf4j
public class TestLensDimensionCommands extends LensCliApplicationTest {

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
    getCommand().createDimension(new File(dimensionSpec.toURI()));
  }

  /**
   * Test dimension commands.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testDimensionCommands() throws Exception {
    log.debug("Starting to test dimension commands");
    URL dimensionSpec = TestLensDimensionCommands.class.getClassLoader().getResource("test-dimension.xml");
    String dimensionList = getCommand().showDimensions();
    Assert.assertFalse(dimensionList.contains("test_dim"));
    createDimension();
    dimensionList = getCommand().showDimensions();
    Assert.assertTrue(dimensionList.contains("test_dim"));
    testFields(getCommand());
    testJoinChains(getCommand());
    testUpdateCommand(new File(dimensionSpec.toURI()), getCommand());
    getCommand().dropDimension("test_dim");
    dimensionList = getCommand().showDimensions();
    Assert.assertFalse(dimensionList.contains("test_dim"));
  }

  private void testJoinChains(LensDimensionCommands command) {
    assertEquals(command.showJoinChains("test_dim"), new XJoinChainTable(new XJoinChains()).toString());
  }

  private void testFields(LensDimensionCommands qCom) {
    String testDimFields = qCom.showQueryableFields("test_dim", true);
    for (String field : Arrays.asList("detail", "id", "d2id", "name")) {
      assertTrue(testDimFields.contains(field));
    }
    assertFalse(testDimFields.contains("measure"));
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

      xmlContent = xmlContent.replace("<property name=\"test_dim.prop\" value=\"test\" />\n",
        "<property name=\"test_dim.prop\" value=\"test\" />"
          + "\n<property name=\"test_dim.prop1\" value=\"test1\" />\n");

      File newFile = new File("target/test_dim1.xml");
      Writer writer = new OutputStreamWriter(new FileOutputStream(newFile));
      writer.write(xmlContent);
      writer.close();

      String desc = command.describeDimension("test_dim");
      log.debug(desc);
      String propString = "name : test_dim.prop  value : test";
      String propString1 = "name : test_dim.prop1  value : test1";
      Assert.assertTrue(desc.contains(propString));

      command.updateDimension("test_dim", new File("target/test_dim1.xml"));
      desc = command.describeDimension("test_dim");
      log.debug(desc);
      Assert.assertTrue(desc.contains(propString));

      Assert.assertTrue(desc.contains(propString1));

      newFile.delete();

    } catch (Throwable t) {
      log.error("Testing update dimension failed with exception", t);
      Assert.fail("Testing update dimension failed with exception" + t.getMessage());
    }
  }
}
