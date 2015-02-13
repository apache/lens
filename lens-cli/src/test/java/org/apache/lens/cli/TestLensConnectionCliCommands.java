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

import java.io.File;
import java.io.IOException;

import javax.ws.rs.BadRequestException;

import org.apache.lens.cli.commands.LensConnectionCommands;
import org.apache.lens.client.LensClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * The Class TestLensConnectionCliCommands.
 */
public class TestLensConnectionCliCommands extends LensCliApplicationTest {

  /** The Constant LOG. */
  private static final Logger LOG = LoggerFactory.getLogger(TestLensConnectionCliCommands.class);

  /**
   * Test client creation.
   */
  @Test
  public void testClientCreation() {
    LensClient client = null;
    try {
      client = new LensClient();
    } catch (Throwable t) {
      Assert.fail("Client should have been able to create a connection to server");
    } finally {
      if (client != null) {
        client.closeConnection();
      }
    }
  }

  /**
   * Test connection command.
   */
  @Test
  public void testConnectionCommand() {
    LensClient client = new LensClient();
    LensConnectionCommands commands = new LensConnectionCommands();
    commands.setClient(client);
    String key = "connectiontest1";
    String value = "connectiontest1val";
    String keyvalList = commands.showParameters();

    Assert.assertFalse(keyvalList.contains("connectiontest1"));

    commands.setParam(key + "=" + value);
    String val = commands.getParam(key);
    Assert.assertEquals(val, key + "=" + value);
    commands.quitShell();
  }

  private File createNewPath(String fileName) {
    File f = new File(fileName);
    try {
      f.createNewFile();
    } catch (IOException e) {
      Assert.fail("Unable to create test file, so bailing out.");
    }
    return f;
  }

  /**
   * Test file commands.
   */
  @Test
  public void testFileCommands() {
    LensClient client = new LensClient();
    LensConnectionCommands commands = new LensConnectionCommands();
    commands.setClient(client);
    LOG.debug("Testing set/remove file operations");

    File f = null;
    try {
      String filename = "/tmp/data";
      f = createNewPath(filename);

      String result = commands.addFile(filename);
      Assert.assertEquals("Add resource succeeded", result);

      result = commands.removeFile(filename);
      Assert.assertEquals("Delete resource succeeded", result);
      LOG.debug("Testing set/remove file operation done");
    } finally {
      if (f != null) {
        f.delete();
      }
      commands.quitShell();
    }
  }

  /**
   * Test jar commands.
   */
  @Test
  public void testJarCommands() {
    LensClient client = new LensClient();
    LensConnectionCommands commands = new LensConnectionCommands();
    commands.setClient(client);
    LOG.debug("Testing set/remove file operations");

    File jar = null;
    try {
      String filename = "/tmp/data.jar";
      jar = createNewPath(filename);

      String result = commands.addJar(filename);
      Assert.assertEquals("Add resource succeeded", result);

      result = commands.removeJar(filename);
      Assert.assertEquals("Delete resource succeeded", result);
      LOG.debug("Testing set/remove file operation done");
    } finally {
      if (jar != null) {
        jar.delete();
      }
      commands.quitShell();
    }
  }

  /**
   * Test list resources commands.
   */
  @Test
  public void testListResourcesCommands() {
    LensClient client = new LensClient();
    LensConnectionCommands commands = new LensConnectionCommands();
    commands.setClient(client);
    LOG.debug("Testing set/remove file operations");

    File file = null;
    File jar = null;
    try {
      String fileName = "/tmp/data.txt";
      file = createNewPath(fileName);
      commands.addFile(fileName);

      String jarName = "/tmp/data.jar";
      jar = createNewPath(jarName);
      commands.addJar(jarName);

      String fileResourcesList = commands.listResources("file");
      Assert.assertEquals(fileResourcesList.split("\n").length, 1);
      Assert.assertTrue(fileResourcesList.split("\n")[0].contains("/tmp/data.txt"));

      String jarResourcesList = commands.listResources("jar");
      Assert.assertEquals(jarResourcesList.split("\n").length, 1);
      Assert.assertTrue(jarResourcesList.split("\n")[0].contains("/tmp/data.jar"));

      String allResources = commands.listResources(null);
      Assert.assertEquals(allResources.split("\n").length, 2);

      Throwable th = null;
      try {
        commands.listResources("invalid-resource-type");
      } catch (Exception e) {
        th = e;
      }
      Assert.assertTrue(th instanceof BadRequestException);

      commands.removeFile(fileName);
      commands.removeJar(jarName);
      LOG.debug("Testing set/remove file operation done");
    } finally {
      if (file != null) {
        file.delete();
      }
      if (jar != null) {
        jar.delete();
      }
      commands.quitShell();
    }
  }
}
