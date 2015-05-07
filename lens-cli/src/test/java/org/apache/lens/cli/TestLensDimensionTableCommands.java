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

import javax.ws.rs.NotFoundException;

import org.apache.lens.cli.commands.LensDimensionCommands;
import org.apache.lens.cli.commands.LensDimensionTableCommands;
import org.apache.lens.client.LensClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * The Class TestLensDimensionTableCommands.
 */
public class TestLensDimensionTableCommands extends LensCliApplicationTest {

  /** The Constant LOG. */
  private static final Logger LOG = LoggerFactory.getLogger(TestLensDimensionTableCommands.class);

  /** The Constant DIM_LOCAL. */
  public static final String DIM_LOCAL = "dim_local";

  /** The command. */
  private static LensDimensionTableCommands command = null;
  private static LensDimensionCommands dimensionCommand = null;

  private static LensDimensionTableCommands getCommand() {
    if (command == null) {
      LensClient client = new LensClient();
      command = new LensDimensionTableCommands();
      command.setClient(client);
    }
    return command;
  }

  private static LensDimensionCommands getDimensionCommand() {
    if (dimensionCommand == null) {
      LensClient client = new LensClient();
      dimensionCommand = new LensDimensionCommands();
      dimensionCommand.setClient(client);
    }
    return dimensionCommand;
  }


  /**
   * Test dim table commands.
   *
   * @throws IOException
   * @throws URISyntaxException
   */
  @Test
  public void testDimTableCommands() throws IOException, URISyntaxException {
    createDimension();
    addDim1Table("dim_table2", "dim_table2.xml", DIM_LOCAL);
    updateDim1Table();
    testDimStorageActions();
    testDimPartitionActions();
    dropDim1Table();
    dropDimension();
  }

  private void dropDimension() {
    getDimensionCommand().dropDimension("test_dim");
  }

  private void createDimension() throws URISyntaxException {
    URL dimensionSpec = TestLensDimensionTableCommands.class.getClassLoader().getResource("test-dimension.xml");
    getDimensionCommand().createDimension(new File(dimensionSpec.toURI()).getAbsolutePath());

  }

  /**
   * Adds the dim1 table.
   *
   * @param tableName   the table name
   * @param specName    the spec name
   * @param storageName the storage name
   * @throws IOException
   */
  public static synchronized void addDim1Table(String tableName, String specName, String storageName)
    throws IOException {
    LensDimensionTableCommands command = getCommand();
    String dimList = command.showDimensionTables(null);
    Assert.assertEquals(dimList, "No dimensiontable found");
    Assert.assertEquals(command.showDimensionTables("test_dim"), "No dimensiontable found for test_dim");

    // add local storage before adding fact table
    TestLensStorageCommands.addLocalStorage(storageName);
    URL dimSpec = TestLensDimensionTableCommands.class.getClassLoader().getResource(specName);

    try {
      command.createDimensionTable(new File(dimSpec.toURI()).getAbsolutePath());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("Unable to create dimtable" + e.getMessage());
    }

    dimList = command.showDimensionTables(null);
    Assert.assertEquals(command.showDimensionTables("test_dim"), dimList);
    try {
      Assert.assertEquals(command.showDimensionTables("blah"), dimList);
      Assert.fail();
    } catch (NotFoundException e) {
      LOG.info("blah is not a table", e);
    }
    try {
      Assert.assertEquals(command.showDimensionTables("dim_table2"), dimList);
      Assert.fail();
    } catch (NotFoundException e) {
      LOG.info("dim_table2 is a table, but not a dimension", e);
    }
    Assert.assertTrue(dimList.contains(tableName), "dim_table table should be found");
  }

  /**
   * Update dim1 table.
   *
   * @throws IOException
   */
  private static void updateDim1Table() throws IOException {
    LensDimensionTableCommands command = getCommand();
    URL dimSpec = TestLensFactCommands.class.getClassLoader().getResource("dim_table2.xml");
    StringBuilder sb = new StringBuilder();
    BufferedReader bufferedReader = new BufferedReader(new FileReader(dimSpec.getFile()));
    String s;
    while ((s = bufferedReader.readLine()) != null) {
      sb.append(s).append("\n");
    }

    bufferedReader.close();

    String xmlContent = sb.toString();

    xmlContent = xmlContent.replace("<property name=\"dim2.prop\" value=\"d2\" />",
      "<property name=\"dim2.prop\" value=\"d1\"/>" + "\n<property name=\"dim2.prop1\" value=\"d2\"/>\n");

    File newFile = new File("/tmp/local-dim1.xml");
    try {
      Writer writer = new OutputStreamWriter(new FileOutputStream(newFile));
      writer.write(xmlContent);
      writer.close();

      String desc = command.describeDimensionTable("dim_table2");
      LOG.debug(desc);
      String propString = "name : dim2.prop  value : d2";
      String propString1 = "name : dim2.prop  value : d1";
      String propString2 = "name : dim2.prop1  value : d2";
      Assert.assertTrue(desc.contains(propString));

      command.updateDimensionTable("dim_table2", "/tmp/local-dim1.xml");
      desc = command.describeDimensionTable("dim_table2");
      LOG.debug(desc);
      Assert.assertTrue(desc.contains(propString1));
      Assert.assertTrue(desc.contains(propString2));

    } finally {
      newFile.delete();
    }
  }

  /**
   * Test dim storage actions.
   *
   * @throws URISyntaxException
   */
  private static void testDimStorageActions() throws URISyntaxException {
    LensDimensionTableCommands command = getCommand();
    String result = command.getDimStorages("dim_table2");
    Assert.assertEquals(DIM_LOCAL, result);
    command.dropAllDimStorages("dim_table2");
    result = command.getDimStorages("dim_table2");
    Assert.assertEquals(result, "No storage found for dim_table2");
    addLocalStorageToDim();
    result = command.getDimStorages("dim_table2");
    Assert.assertNotEquals(result, "No storage found for dim_table2");
    command.dropStorageFromDim("dim_table2", DIM_LOCAL);
    result = command.getDimStorages("dim_table2");
    Assert.assertEquals(result, "No storage found for dim_table2");
    addLocalStorageToDim();
  }

  /**
   * Adds the local storage to dim.
   *
   * @throws URISyntaxException
   */
  private static void addLocalStorageToDim() throws URISyntaxException {
    LensDimensionTableCommands command = getCommand();
    String result;
    URL resource = TestLensDimensionTableCommands.class.getClassLoader().getResource("dim-local-storage-element.xml");
    command.addNewDimStorage("dim_table2", new File(resource.toURI()).getAbsolutePath());
    result = command.getDimStorages("dim_table2");
    Assert.assertEquals(DIM_LOCAL, result);

    result = command.getStorageFromDim("dim_table2", DIM_LOCAL);
    String partString = "DAILY";
    Assert.assertTrue(result.contains(partString));
  }

  /**
   * Test dim partition actions.
   */
  private static void testDimPartitionActions() {
    LensDimensionTableCommands command = getCommand();
    String result;
    result = command.getAllPartitionsOfDim("dim_table2", DIM_LOCAL, null);
    Assert.assertTrue(result.trim().isEmpty());
    try {
      command.addPartitionToDimtable("dim_table2", DIM_LOCAL, new File(
        TestLensFactCommands.class.getClassLoader().getResource("dim1-local-part.xml").toURI()).getAbsolutePath());
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Unable to locate the storage part file for adding new storage to dim table dim_table2");
    }
    verifyAndDeletePartition();
    try {
      command.addPartitionsToDimtable("dim_table2", DIM_LOCAL, new File(
        TestLensFactCommands.class.getClassLoader().getResource("dim1-local-parts.xml").toURI()).getAbsolutePath());
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Unable to locate the storage part file for adding new storage to dim table dim_table2");
    }
    verifyAndDeletePartition();
  }

  private static void verifyAndDeletePartition() {
    String result = command.getAllPartitionsOfDim("dim_table2", DIM_LOCAL, null);
    String partString = "DAILY";
    Assert.assertTrue(result.contains(partString));
    command.dropAllPartitionsOfDim("dim_table2", DIM_LOCAL, null);
    result = command.getAllPartitionsOfDim("dim_table2", DIM_LOCAL, null);
    Assert.assertTrue(result.trim().isEmpty());
  }

  /**
   * Adds the partition to storage.
   *
   * @param tableName     the table name
   * @param storageName   the storage name
   * @param localPartSpec the local part spec
   */
  public static void addPartitionToStorage(String tableName, String storageName, String localPartSpec) {
    LensDimensionTableCommands command = getCommand();
    URL resource = TestLensFactCommands.class.getClassLoader().getResource(localPartSpec);
    try {
      command.addPartitionToDimtable(tableName, storageName, new File(resource.toURI()).getAbsolutePath());
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Unable to locate the storage part file for adding new storage to dim table dim_table2");
    }
  }

  /**
   * Drop dim1 table.
   */
  public static void dropDim1Table() {
    LensDimensionTableCommands command = getCommand();
    String dimList = command.showDimensionTables(null);
    Assert.assertEquals("dim_table2", dimList, "dim table should be found");
    command.dropDimensionTable("dim_table2", false);
    dimList = command.showDimensionTables(null);
    Assert.assertEquals(dimList, "No dimensiontable found", "Dim tables should not be found");
    TestLensStorageCommands.dropStorage(DIM_LOCAL);
  }
}
