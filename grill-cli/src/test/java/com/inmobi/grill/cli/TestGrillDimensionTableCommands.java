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


import com.inmobi.grill.cli.commands.GrillDimensionTableCommands;
import com.inmobi.grill.client.GrillClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;
import java.net.URL;

public class TestGrillDimensionTableCommands extends GrillCliApplicationTest {

  private static final Logger LOG = LoggerFactory.getLogger(TestGrillDimensionTableCommands.class);
  public static final String DIM_LOCAL = "dim_local";
  private static GrillDimensionTableCommands command = null;


  private static GrillDimensionTableCommands getCommand() {
    if (command == null) {
      GrillClient client = new GrillClient();
      command = new GrillDimensionTableCommands();
      command.setClient(client);
    }
    return command;
  }


  @Test
  public void testDimTableCommands() {
    addDim1Table("dim_table2",
        "dim_table2.xml",
        "dim2-storage-spec.xml", DIM_LOCAL);
    updateDim1Table();
    testDimStorageActions();
    testDimPartitionActions();
    dropDim1Table();
  }


  public static void addDim1Table(String tableName,String specName, String storageSpecName, String storageName) {
    GrillDimensionTableCommands command = getCommand();
    String dimList = command.showDimensionTables();
    Assert.assertEquals("No Dimensions Found", dimList,
        "Dim tables should not be found");
    //add local storage before adding fact table
    TestGrillStorageCommands.addLocalStorage(storageName);
    URL dimSpec =
        TestGrillDimensionTableCommands.class.getClassLoader().getResource(specName);
    URL factStorageSpec =
        TestGrillDimensionTableCommands.class.getClassLoader().getResource(storageSpecName);

    try {
      command.createDimensionTable(new File(dimSpec.toURI()).getAbsolutePath()
          + " " + new File(factStorageSpec.toURI()).getAbsolutePath());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("Unable to create dimtable" + e.getMessage());
    }

    dimList = command.showDimensionTables();
    Assert.assertEquals(tableName, dimList, "dim_table table should be found");
  }

  private static void updateDim1Table() {
    try {
      GrillDimensionTableCommands command = getCommand();
      URL dimSpec =
          TestGrillFactCommands.class.getClassLoader().getResource("dim_table2.xml");
      StringBuilder sb = new StringBuilder();
      BufferedReader bufferedReader = new BufferedReader(new FileReader(dimSpec.getFile()));
      String s;
      while ((s = bufferedReader.readLine()) != null) {
        sb.append(s).append("\n");
      }

      bufferedReader.close();

      String xmlContent = sb.toString();

      xmlContent = xmlContent.replace("<properties name=\"dim2.prop\" value=\"d2\"/>\n",
          "<properties name=\"dim2.prop\" value=\"d1\"/>" +
              "\n<properties name=\"dim2.prop1\" value=\"d2\"/>\n");

      File newFile = new File("/tmp/local-dim1.xml");
      Writer writer = new OutputStreamWriter(new FileOutputStream(newFile));
      writer.write(xmlContent);
      writer.close();

      String desc = command.describeDimensionTable("dim_table2");
      LOG.debug(desc);
      Assert.assertTrue(desc.contains("dim2.prop=d2"));

      command.updateDimensionTable("dim_table2 /tmp/local-dim1.xml");
      desc = command.describeDimensionTable("dim_table2");
      LOG.debug(desc);
      Assert.assertTrue(desc.contains("dim2.prop=d1"));
      Assert.assertTrue(desc.contains("dim2.prop1=d2"));
      newFile.delete();

    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Updating of the dim-table2 table failed with "+t.getMessage());
    }
  }

  private static void testDimStorageActions() {
    GrillDimensionTableCommands command = getCommand();
    String result = command.getDimStorages("dim_table2");
    Assert.assertEquals(DIM_LOCAL, result);
    command.dropAllDimStorages("dim_table2");
    result = command.getDimStorages("dim_table2");
    Assert.assertEquals("No storages found for dim_table2", result);
    addLocalStorageToDim();
    command.dropStorageFromDim("dim_table2 "+ DIM_LOCAL);
    result = command.getDimStorages("dim_table2");
    Assert.assertEquals("No storages found for dim_table2", result);
    addLocalStorageToDim();
  }

  private static void addLocalStorageToDim() {
    GrillDimensionTableCommands command = getCommand();
    String result;
    URL resource = TestGrillFactCommands.class.getClassLoader().getResource("dim-local-storage-element.xml");
    try {
      command.addNewDimStorage("dim_table2 " + new File(resource.toURI()).getAbsolutePath());
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Unable to locate the storage part file for adding new storage to fact table fact1");
    }
    result = command.getDimStorages("dim_table2");
    Assert.assertEquals(DIM_LOCAL, result);

    result = command.getStorageFromDim("dim_table2 "+ DIM_LOCAL);
    Assert.assertTrue(result.contains("Update Period : HOURLY"));
  }


  private static void testDimPartitionActions() {
    GrillDimensionTableCommands command = getCommand();
    String result;
    result = command.getAllPartitionsOfDim("dim_table2 "+ DIM_LOCAL);
    Assert.assertTrue(result.trim().isEmpty());
    addPartitionToStorage("dim_table2", DIM_LOCAL,"dim1-local-part.xml");
    result = command.getAllPartitionsOfDim("dim_table2 "+ DIM_LOCAL);
    Assert.assertTrue(result.contains("Update Period: HOURLY"));
    command.dropAllPartitionsOfDim("dim_table2 "+ DIM_LOCAL);
    result = command.getAllPartitionsOfDim("dim_table2 " + DIM_LOCAL);
    Assert.assertTrue(result.trim().isEmpty());

  }

  public static void addPartitionToStorage(String tableName, String storageName, String localPartSpec) {
    GrillDimensionTableCommands command = getCommand();
    URL resource = TestGrillFactCommands.class.getClassLoader().getResource(localPartSpec);
    try {
      command.addPartitionToFact(tableName+" "+ storageName +" " + new File(resource.toURI()).getAbsolutePath());
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Unable to locate the storage part file for adding new storage to dim table dim_table2");
    }
  }

  public static void dropDim1Table() {
    GrillDimensionTableCommands command = getCommand();
    String dimList = command.showDimensionTables();
    Assert.assertEquals("dim_table2", dimList, "dim_table table should be found");
    command.dropDimensionTable("dim_table2", false);
    dimList = command.showDimensionTables();
    Assert.assertEquals("No Dimensions Found", dimList,
        "Dim tables should not be found");
    TestGrillStorageCommands.dropStorage(DIM_LOCAL);
  }
}
