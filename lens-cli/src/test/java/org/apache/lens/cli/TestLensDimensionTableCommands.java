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

import org.apache.lens.cli.commands.LensDimensionTableCommands;
import org.apache.lens.client.LensClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;
import java.net.URL;

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

  private static LensDimensionTableCommands getCommand() {
    if (command == null) {
      LensClient client = new LensClient();
      command = new LensDimensionTableCommands();
      command.setClient(client);
    }
    return command;
  }

  /**
   * Test dim table commands.
   */
  @Test
  public void testDimTableCommands() {
    addDim1Table("dim_table2", "dim_table2.xml", "dim2-storage-spec.xml", DIM_LOCAL);
    updateDim1Table();
    testDimStorageActions();
    testDimPartitionActions();
    dropDim1Table();
  }

  /**
   * Adds the dim1 table.
   *
   * @param tableName
   *          the table name
   * @param specName
   *          the spec name
   * @param storageSpecName
   *          the storage spec name
   * @param storageName
   *          the storage name
   */
  public static void addDim1Table(String tableName, String specName, String storageSpecName, String storageName) {
    LensDimensionTableCommands command = getCommand();
    String dimList = command.showDimensionTables();
    Assert.assertEquals("No Dimensions Found", dimList, "Dim tables should not be found");
    // add local storage before adding fact table
    TestLensStorageCommands.addLocalStorage(storageName);
    URL dimSpec = TestLensDimensionTableCommands.class.getClassLoader().getResource(specName);
    URL factStorageSpec = TestLensDimensionTableCommands.class.getClassLoader().getResource(storageSpecName);

    try {
      command.createDimensionTable(new File(dimSpec.toURI()).getAbsolutePath() + " "
          + new File(factStorageSpec.toURI()).getAbsolutePath());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("Unable to create dimtable" + e.getMessage());
    }

    dimList = command.showDimensionTables();
    Assert.assertEquals(tableName, dimList, "dim_table table should be found");
  }

  /**
   * Update dim1 table.
   */
  private static void updateDim1Table() {
    try {
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

      xmlContent = xmlContent.replace("<properties name=\"dim2.prop\" value=\"d2\"/>\n",
          "<properties name=\"dim2.prop\" value=\"d1\"/>" + "\n<properties name=\"dim2.prop1\" value=\"d2\"/>\n");

      File newFile = new File("/tmp/local-dim1.xml");
      Writer writer = new OutputStreamWriter(new FileOutputStream(newFile));
      writer.write(xmlContent);
      writer.close();

      String desc = command.describeDimensionTable("dim_table2");
      LOG.debug(desc);
      String propString = "name : dim2.prop  value : d2";
      String propString1 = "name : dim2.prop  value : d1";
      String propString2 = "name : dim2.prop1  value : d2";
      Assert.assertTrue(desc.contains(propString));

      command.updateDimensionTable("dim_table2 /tmp/local-dim1.xml");
      desc = command.describeDimensionTable("dim_table2");
      LOG.debug(desc);
      Assert.assertTrue(desc.contains(propString1));
      Assert.assertTrue(desc.contains(propString2));
      newFile.delete();

    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Updating of the dim-table2 table failed with " + t.getMessage());
    }
  }

  /**
   * Test dim storage actions.
   */
  private static void testDimStorageActions() {
    LensDimensionTableCommands command = getCommand();
    String result = command.getDimStorages("dim_table2");
    Assert.assertEquals(DIM_LOCAL, result);
    command.dropAllDimStorages("dim_table2");
    result = command.getDimStorages("dim_table2");
    Assert.assertEquals("No storages found for dim_table2", result);
    addLocalStorageToDim();
    command.dropStorageFromDim("dim_table2 " + DIM_LOCAL);
    result = command.getDimStorages("dim_table2");
    Assert.assertEquals("No storages found for dim_table2", result);
    addLocalStorageToDim();
  }

  /**
   * Adds the local storage to dim.
   */
  private static void addLocalStorageToDim() {
    LensDimensionTableCommands command = getCommand();
    String result;
    URL resource = TestLensFactCommands.class.getClassLoader().getResource("dim-local-storage-element.xml");
    try {
      command.addNewDimStorage("dim_table2 " + new File(resource.toURI()).getAbsolutePath());
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Unable to locate the storage part file for adding new storage to fact table fact1");
    }
    result = command.getDimStorages("dim_table2");
    Assert.assertEquals(DIM_LOCAL, result);

    result = command.getStorageFromDim("dim_table2 " + DIM_LOCAL);
    String partString = "HOURLY";
    Assert.assertTrue(result.contains(partString));
  }

  /**
   * Test dim partition actions.
   */
  private static void testDimPartitionActions() {
    LensDimensionTableCommands command = getCommand();
    String result;
    result = command.getAllPartitionsOfDim("dim_table2 " + DIM_LOCAL);
    Assert.assertTrue(result.trim().isEmpty());
    addPartitionToStorage("dim_table2", DIM_LOCAL, "dim1-local-part.xml");
    result = command.getAllPartitionsOfDim("dim_table2 " + DIM_LOCAL);
    String partString = "HOURLY";
    Assert.assertTrue(result.contains(partString));
    command.dropAllPartitionsOfDim("dim_table2 " + DIM_LOCAL);
    result = command.getAllPartitionsOfDim("dim_table2 " + DIM_LOCAL);
    Assert.assertTrue(result.trim().isEmpty());
  }

  /**
   * Adds the partition to storage.
   *
   * @param tableName
   *          the table name
   * @param storageName
   *          the storage name
   * @param localPartSpec
   *          the local part spec
   */
  public static void addPartitionToStorage(String tableName, String storageName, String localPartSpec) {
    LensDimensionTableCommands command = getCommand();
    URL resource = TestLensFactCommands.class.getClassLoader().getResource(localPartSpec);
    try {
      command.addPartitionToFact(tableName + " " + storageName + " " + new File(resource.toURI()).getAbsolutePath());
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
    String dimList = command.showDimensionTables();
    Assert.assertEquals("dim_table2", dimList, "dim_table table should be found");
    command.dropDimensionTable("dim_table2", false);
    dimList = command.showDimensionTables();
    Assert.assertEquals("No Dimensions Found", dimList, "Dim tables should not be found");
    TestLensStorageCommands.dropStorage(DIM_LOCAL);
  }
}
