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


import org.apache.lens.cli.commands.GrillFactCommands;
import org.apache.lens.client.GrillClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;
import java.net.URL;

public class TestGrillFactCommands extends GrillCliApplicationTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestGrillFactCommands.class);
  public static final String FACT_LOCAL = "fact_local";
  private static GrillFactCommands command = null;

  @Test
  public void testFactCommands() {
    addFact1Table();
    updateFact1Table();
    testFactStorageActions();
    testFactPartitionActions();
    dropFact1Table();
  }



  private static GrillFactCommands getCommand() {
    if(command == null ) {
      GrillClient client = new GrillClient();
      command = new GrillFactCommands();
      command.setClient(client);
    }
    return command;
  }


  public static void addFact1Table() {
    GrillFactCommands command = getCommand();
    String factList = command.showFacts();
    Assert.assertEquals("No Facts Found", factList,
        "Fact tables should not be found");
    //add local storage before adding fact table
    TestGrillStorageCommands.addLocalStorage(FACT_LOCAL);
    URL factSpec =
        TestGrillFactCommands.class.getClassLoader().getResource("fact1.xml");
    URL factStorageSpec =
        TestGrillFactCommands.class.getClassLoader().getResource("fact1-storage-spec.xml");
    try {
      command.createFact(new File(factSpec.toURI()).getAbsolutePath()
          + " " + new File(factStorageSpec.toURI()).getAbsolutePath());
    } catch (Exception e) {
      Assert.fail("Unable to create fact table" + e.getMessage());
    }
    factList = command.showFacts();
    Assert.assertEquals("fact1", factList, "Fact1 table should be found");
  }

  public static void updateFact1Table() {
    try {
      GrillFactCommands command = getCommand();
      URL factSpec =
          TestGrillFactCommands.class.getClassLoader().getResource("fact1.xml");
      StringBuilder sb = new StringBuilder();
      BufferedReader bufferedReader = new BufferedReader(new FileReader(factSpec.getFile()));
      String s;
      while ((s = bufferedReader.readLine()) != null) {
        sb.append(s).append("\n");
      }

      bufferedReader.close();

      String xmlContent = sb.toString();

      xmlContent = xmlContent.replace("<properties name=\"fact1.prop\" value=\"f1\"/>\n",
          "<properties name=\"fact1.prop\" value=\"f1\"/>" +
              "\n<properties name=\"fact1.prop1\" value=\"f2\"/>\n");

      File newFile = new File("/tmp/local-fact1.xml");
      Writer writer = new OutputStreamWriter(new FileOutputStream(newFile));
      writer.write(xmlContent);
      writer.close();

      String desc = command.describeFactTable("fact1");
      LOG.debug(desc);
      String propString = "name : fact1.prop  value : f1";
      String propString1 = "name : fact1.prop1  value : f2";

      Assert.assertTrue(desc.contains(propString));

      command.updateFactTable("fact1 /tmp/local-fact1.xml");
      desc = command.describeFactTable("fact1");
      LOG.debug(desc);
      Assert.assertTrue(
          desc.contains(propString),"The sample property value is not set");

      Assert.assertTrue(
          desc.contains(propString1),"The sample property value is not set");

      newFile.delete();

    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Updating of the fact1 table failed with "+t.getMessage());
    }

  }


  private static void testFactStorageActions() {
    GrillFactCommands command = getCommand();
    String result = command.getFactStorages("fact1");
    Assert.assertEquals(FACT_LOCAL, result);
    command.dropAllFactStorages("fact1");
    result = command.getFactStorages("fact1");
    Assert.assertEquals("No storages found for fact1", result);
    addLocalStorageToFact1();
    command.dropStorageFromFact("fact1 "+ FACT_LOCAL);
    result = command.getFactStorages("fact1");
    Assert.assertEquals("No storages found for fact1", result);
    addLocalStorageToFact1();
  }

  private static void addLocalStorageToFact1() {
    GrillFactCommands command = getCommand();
    String result;
    URL resource = TestGrillFactCommands.class.getClassLoader().getResource("fact-local-storage-element.xml");
    try {
      command.addNewFactStorage("fact1 " + new File(resource.toURI()).getAbsolutePath());
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Unable to locate the storage part file for adding new storage to fact table fact1");
    }
    result = command.getFactStorages("fact1");
    Assert.assertEquals(FACT_LOCAL, result);

    result = command.getStorageFromFact("fact1 "+ FACT_LOCAL);
    Assert.assertTrue(result.contains("HOURLY"));
    Assert.assertTrue(result.contains("DAILY"));

  }


  private void testFactPartitionActions() {
    GrillFactCommands command = getCommand();
    String result;
    result = command.getAllPartitionsOfFact("fact1 "+FACT_LOCAL);
    Assert.assertTrue(result.trim().isEmpty());
    URL resource = TestGrillFactCommands.class.getClassLoader().getResource("fact1-local-part.xml");
    try {
      command.addPartitionToFact("fact1 "+ FACT_LOCAL +" " + new File(resource.toURI()).getAbsolutePath());
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Unable to locate the storage part file for adding new storage to fact table fact1");
    }
    result = command.getAllPartitionsOfFact("fact1 "+ FACT_LOCAL);
    Assert.assertTrue(result.contains("HOURLY"));
    command.dropAllPartitionsOfFact("fact1 "+ FACT_LOCAL);
    result = command.getAllPartitionsOfFact("fact1 "+ FACT_LOCAL);
    Assert.assertTrue(result.trim().isEmpty());
  }

  public static void dropFact1Table() {
    GrillFactCommands command = getCommand();
    String factList = command.showFacts();
    Assert.assertEquals("fact1", factList, "Fact1 table should be found");
    command.dropFact("fact1", false);
    factList = command.showFacts();
    Assert.assertEquals("No Facts Found", factList,
        "Fact tables should not be found");
    TestGrillStorageCommands.dropStorage(FACT_LOCAL);
  }
}
