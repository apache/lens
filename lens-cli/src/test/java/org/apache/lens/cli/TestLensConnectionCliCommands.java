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
import java.net.URI;

import javax.ws.rs.BadRequestException;

import org.apache.lens.api.LensSessionHandle;
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
      if (!f.exists()) {
        f.createNewFile();
      }
    } catch (IOException e) {
      Assert.fail("Unable to create test file, so bailing out.");
    }
    return f;
  }

  private String getFilePathFromUri(String uripath) {
    try {
      return new URI(uripath).getPath();
    } catch (Exception e) {
      return null;
    }
  }

  private File createNewFile(String fileName) {
    File f = null;
    try {
      String filepath = getFilePathFromUri(fileName);
      Assert.assertNotNull(fileName, "Unable to get filepath from uri pattern.");
      f = new File(filepath);
      if (!f.exists()) {
        f.createNewFile();
      }
    } catch (Exception e) {
      Assert.fail("Unable to create test file, so bailing out.");
    }
    return f;
  }

  private File deleteFile(String fileName) {
    File f = null;
    try {
      String filepath = getFilePathFromUri(fileName);
      Assert.assertNotNull(fileName, "Unable to get filepath from uri pattern.");
      f = new File(filepath);
      if (f.exists()) {
        f.delete();
      }
    } catch (Exception e) {
      Assert.fail("Unable to delete test file, so bailing out.");
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
      String filename = "target/data";
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
   * Test file commands with URI in regex.
   */
  @Test
  public void testFileCommandsWithURIRegex() {
    LensClient client = new LensClient();
    LensConnectionCommands commands = new LensConnectionCommands();
    commands.setClient(client);
    LOG.debug("Testing set/remove file operations");

    java.io.File file = new java.io.File("");
    String projectdir = file.getAbsolutePath();

    /* Tests input file pattern file: and file://  */
    String filenameA = "file:" + projectdir + "/target/tempdata_a.txt";
    String filenameB = "file://" + projectdir + "/target/tempdata_b.txt";

    String fileRegex = "file:" + projectdir + "/target/tempdata_*.txt";

    try {
      createNewFile(filenameA);
      createNewFile(filenameB);

      String result = commands.addFile(fileRegex);
      Assert.assertEquals("Add resource succeeded", result);

      result = commands.removeFile(fileRegex);
      Assert.assertEquals("Delete resource succeeded", result);

      LOG.debug("Testing set/remove file operation done");
    } finally {
      deleteFile(filenameA);
      deleteFile(filenameB);
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
      String filename = "target/data.jar";
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
   * Test jar commands with regex specified.
   */
  @Test
  public void testResourceCommandsWithRegex() {
    LensClient client = new LensClient();
    LensConnectionCommands commands = new LensConnectionCommands();
    commands.setClient(client);
    LOG.debug("Testing set/remove file operations");

    File fileA = null, fileB = null;
    String filenameA, filenameB, fileRegex, result;
    try {
      filenameA = "target/tempdata_a";
      filenameB = "target/tempdata_b";
      fileRegex = "target/tempdata_*";

      fileA = createNewPath(filenameA);
      fileB = createNewPath(filenameB);
      result = commands.addFile(fileRegex);
      Assert.assertEquals("Add resource succeeded", result);

      result = commands.removeFile(fileRegex);
      Assert.assertEquals("Delete resource succeeded", result);

      filenameA = "target/tempdata_a.jar";
      filenameB = "target/tempdata_b.jar";
      fileRegex = "target/tempdata_*.jar";

      fileA = createNewPath(filenameA);
      fileB = createNewPath(filenameB);

      result = commands.addJar(fileRegex);
      Assert.assertEquals("Add resource succeeded", result);

      result = commands.removeJar(fileRegex);
      Assert.assertEquals("Delete resource succeeded", result);

      LOG.debug("Testing set/remove resource operation done");
    } finally {
      if (fileA != null) {
        fileA.delete();
      }
      if (fileB != null) {
        fileB.delete();
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
      String fileName = "target/data.txt";
      file = createNewPath(fileName);
      commands.addFile(fileName);

      String jarName = "target/data.jar";
      jar = createNewPath(jarName);
      commands.addJar(jarName);

      String fileResourcesList = commands.listResources("file");
      Assert.assertEquals(fileResourcesList.split("\n").length, 1);
      Assert.assertTrue(fileResourcesList.split("\n")[0].contains("target/data.txt"));

      String jarResourcesList = commands.listResources("jar");
      Assert.assertEquals(jarResourcesList.split("\n").length, 1);
      Assert.assertTrue(jarResourcesList.split("\n")[0].contains("target/data.jar"));

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

  /**
   * Test CLI command to get session handle
   */
  @Test
  public void testGetSessionHandle() {
    LensClient client = new LensClient();
    LensConnectionCommands commands = new LensConnectionCommands();
    commands.setClient(client);
    try {
      LensSessionHandle sessionHandle = client.getConnection().getSessionHandle();
      Assert.assertNotNull(sessionHandle);
      String output = commands.getSessionHandle();
      Assert.assertTrue(output.contains(sessionHandle.getPublicId().toString()), "session handle output: " + output);
    } finally {
      commands.quitShell();
    }
  }
}
