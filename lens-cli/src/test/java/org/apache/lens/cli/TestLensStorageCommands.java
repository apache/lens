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

import org.apache.lens.cli.commands.LensStorageCommands;
import org.apache.lens.client.LensClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;
import java.net.URL;

/**
 * The Class TestLensStorageCommands.
 */
public class TestLensStorageCommands extends LensCliApplicationTest {

  /** The command. */
  private static LensStorageCommands command;

  /** The Constant LOG. */
  private static final Logger LOG = LoggerFactory.getLogger(TestLensStorageCommands.class);

  /**
   * Test storage commands.
   */
  @Test
  public void testStorageCommands() {
    addLocalStorage("local_storage_test");
    testUpdateStorage("local_storage_test");
    dropStorage("local_storage_test");

  }

  private static LensStorageCommands getCommand() {
    if (command == null) {
      LensClient client = new LensClient();
      command = new LensStorageCommands();
      command.setClient(client);
    }
    return command;
  }

  /**
   * Drop storage.
   *
   * @param storageName
   *          the storage name
   */
  public static void dropStorage(String storageName) {
    String storageList;
    LensStorageCommands command = getCommand();
    command.dropStorage(storageName);
    storageList = command.getStorages();
    Assert.assertFalse(storageList.contains(storageName), "Storage list contains " + storageName);
  }

  /**
   * Adds the local storage.
   *
   * @param storageName
   *          the storage name
   */
  public synchronized static void addLocalStorage(String storageName) {
    LensStorageCommands command = getCommand();
    URL storageSpec = TestLensStorageCommands.class.getClassLoader().getResource("local-storage.xml");
    File newFile = new File("/tmp/local-" + storageName + ".xml");
    try {
      StringBuilder sb = new StringBuilder();
      BufferedReader bufferedReader = new BufferedReader(new FileReader(storageSpec.getFile()));
      String s;
      while ((s = bufferedReader.readLine()) != null) {
        sb.append(s).append("\n");
      }

      bufferedReader.close();

      String xmlContent = sb.toString();

      xmlContent = xmlContent.replace("name=\"local\"", "name=\"" + storageName + "\"");

      Writer writer = new OutputStreamWriter(new FileOutputStream(newFile));
      writer.write(xmlContent);
      writer.close();
      LOG.debug("Using Storage spec from file : " + newFile.getAbsolutePath());
      String storageList = command.getStorages();
      Assert.assertFalse(storageList.contains(storageName), " Storage list contains " + storageName
          + " storage list is  " + storageList + " file used is " + newFile.getAbsolutePath());
      command.createStorage(newFile.getAbsolutePath());
      storageList = command.getStorages();
      Assert.assertTrue(storageList.contains(storageName));
    } catch (Exception e) {
      Assert.fail("Unable to add storage " + storageName);
    } finally {
      newFile.delete();
    }
  }

  /**
   * Test update storage.
   *
   * @param storageName
   *          the storage name
   */
  private void testUpdateStorage(String storageName) {

    try {
      LensStorageCommands command = getCommand();
      URL storageSpec = TestLensStorageCommands.class.getClassLoader().getResource("local-storage.xml");
      StringBuilder sb = new StringBuilder();
      BufferedReader bufferedReader = new BufferedReader(new FileReader(storageSpec.getFile()));
      String s;
      while ((s = bufferedReader.readLine()) != null) {
        sb.append(s).append("\n");
      }

      bufferedReader.close();

      String xmlContent = sb.toString();
      xmlContent = xmlContent.replace("name=\"local\"", "name=\"" + storageName + "\"");
      xmlContent = xmlContent.replace("<properties name=\"storage.url\" value=\"file:///\"/>\n",
          "<properties name=\"storage.url\" value=\"file:///\"/>"
              + "\n<properties name=\"sample_cube.prop1\" value=\"sample1\" />\n");

      File newFile = new File("/tmp/" + storageName + ".xml");
      Writer writer = new OutputStreamWriter(new FileOutputStream(newFile));
      writer.write(xmlContent);
      writer.close();

      String desc = command.describeStorage(storageName);
      LOG.debug(desc);
      String propString = "name : storage.url  value : file:///";
      Assert.assertTrue(desc.contains(propString));

      command.updateStorage(storageName + " /tmp/local-storage1.xml");
      desc = command.describeStorage(storageName);
      LOG.debug(desc);
      Assert.assertTrue(desc.contains(propString));
      newFile.delete();

    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Testing update storage failed with exception" + t.getMessage());
    }
  }

}
