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
import java.net.URL;
import java.util.Arrays;

import org.apache.lens.api.metastore.*;
import org.apache.lens.cli.commands.LensCubeCommands;
import org.apache.lens.cli.commands.LensDimensionCommands;
import org.apache.lens.cli.table.XJoinChainTable;
import org.apache.lens.client.LensClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.testng.annotations.Test;

/**
 * The Class TestLensCubeCommands.
 */
public class TestLensCubeCommands extends LensCliApplicationTest {

  /** The Constant LOG. */
  private static final Logger LOG = LoggerFactory.getLogger(TestLensCubeCommands.class);

  /**
   * Test cube commands.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testCubeCommands() throws Exception {
    LensClient client = new LensClient();
    LensDimensionCommands dimensionCommand = new LensDimensionCommands();
    dimensionCommand.setClient(client);
    dimensionCommand.createDimension(new File(
      TestLensCubeCommands.class.getClassLoader().getResource("test-detail.xml").toURI()));
    dimensionCommand.createDimension(new File(
      TestLensCubeCommands.class.getClassLoader().getResource("test-dimension.xml").toURI()));
    LensCubeCommands command = new LensCubeCommands();
    command.setClient(client);
    LOG.debug("Starting to test cube commands");
    URL cubeSpec = TestLensCubeCommands.class.getClassLoader().getResource("sample-cube.xml");
    String cubeList = command.showCubes();
    assertFalse(cubeList.contains("sample_cube"));
    command.createCube(new File(cubeSpec.toURI()));
    cubeList = command.showCubes();
    assertEquals(command.getLatest("sample_cube", "dt"), "No Data Available");
    assertTrue(cubeList.contains("sample_cube"));
    testJoinChains(command);
    testFields(command);
    testUpdateCommand(new File(cubeSpec.toURI()), command);
    command.dropCube("sample_cube");
    try {
      command.getLatest("sample_cube", "dt");
      fail("should have failed as cube doesn't exist");
    } catch (Exception e) {
      //pass
    }
    cubeList = command.showCubes();
    assertFalse(cubeList.contains("sample_cube"));
    dimensionCommand.dropDimension("test_detail");
    dimensionCommand.dropDimension("test_dim");
  }

  private void testJoinChains(LensCubeCommands command) {
    String joinChains = command.showJoinChains("sample_cube");
    XJoinChains chains = new XJoinChains();
    XJoinChain chain1 = new XJoinChain();
    chain1.setPaths(new XJoinPaths());
    XJoinPath path = new XJoinPath();
    path.setEdges(new XJoinEdges());
    XJoinEdge edge1 = new XJoinEdge();
    XTableReference ref1 = new XTableReference();
    ref1.setTable("sample_cube");
    ref1.setColumn("dim2");
    XTableReference ref2 = new XTableReference();
    ref2.setTable("test_detail");
    ref2.setColumn("id");
    edge1.setFrom(ref1);
    edge1.setTo(ref2);
    path.getEdges().getEdge().add(edge1);
    chain1.setName("testdetailchain");
    chain1.getPaths().getPath().add(path);
    chain1.setDestTable("test_detail");
    XJoinChain chain2 = new XJoinChain();
    chain2.setPaths(new XJoinPaths());
    XJoinPath path2 = new XJoinPath();
    path2.setEdges(new XJoinEdges());
    XJoinEdge edge2 = new XJoinEdge();
    XTableReference ref3 = new XTableReference();
    ref3.setTable("sample_cube");
    ref3.setColumn("dim1");
    XTableReference ref4 = new XTableReference();
    ref4.setTable("test_dim");
    ref4.setColumn("id");
    edge2.setFrom(ref3);
    edge2.setTo(ref4);
    path2.getEdges().getEdge().add(edge2);
    chain2.setName("testdimchain");
    chain2.getPaths().getPath().add(path2);
    chain2.setDestTable("test_dim");
    chains.getJoinChain().add(chain2);
    chains.getJoinChain().add(chain1);
    XJoinChains chainsInDiffOrder = new XJoinChains();
    chainsInDiffOrder.getJoinChain().add(chain1);
    chainsInDiffOrder.getJoinChain().add(chain2);
    assertTrue(joinChains.equals(new XJoinChainTable(chains).toString())
        || joinChains.equals(new XJoinChainTable(chainsInDiffOrder).toString()));
  }

  private void testFields(LensCubeCommands command) {
    String fields = command.showQueryableFields("sample_cube", true);
    for (String field : Arrays
      .asList("dim1", "dim2", "dim3", "dimdetail", "measure1", "measure2", "measure3", "measure4", "expr_msr5")) {
      assertTrue(fields.contains(field), fields + " do not contain " + field);
    }
    assertTrue(fields.contains("measure3 + measure4 + 0.01"));
    assertTrue(fields.replace("measure3 + measure4 + 0.01", "blah").contains("measure3 + measure4"));
  }

  /**
   * Test update command.
   *
   * @param f the file
   * @param command
   *          the command
   * @throws IOException
   */
  private void testUpdateCommand(File f, LensCubeCommands command) throws IOException {
    StringBuilder sb = new StringBuilder();
    BufferedReader bufferedReader = new BufferedReader(new FileReader(f));
    String s;
    while ((s = bufferedReader.readLine()) != null) {
      sb.append(s).append("\n");
    }

    bufferedReader.close();

    String xmlContent = sb.toString();

    xmlContent = xmlContent.replace("<property name=\"sample_cube.prop\" value=\"sample\" />\n",
      "<property name=\"sample_cube.prop\" value=\"sample\" />"
        + "\n<property name=\"sample_cube.prop1\" value=\"sample1\" />\n");

    File newFile = new File("target/sample_cube1.xml");
    try {
      Writer writer = new OutputStreamWriter(new FileOutputStream(newFile));
      writer.write(xmlContent);
      writer.close();

      String desc = command.describeCube("sample_cube");
      LensClient client = command.getClient();
      LOG.debug(desc);
      String propString = "name : sample_cube.prop  value : sample";
      String propString1 = "name : sample_cube.prop1  value : sample1";

      assertTrue(desc.contains(propString));

      command.updateCube("sample_cube", new File("target/sample_cube1.xml"));
      desc = command.describeCube("sample_cube");
      LOG.debug(desc);
      assertTrue(desc.contains(propString));
      assertTrue(desc.contains(propString1));
    } finally {
      newFile.delete();
    }
  }
}
