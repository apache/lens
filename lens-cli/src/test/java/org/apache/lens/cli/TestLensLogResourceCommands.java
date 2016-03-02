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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.lens.cli.commands.LensLogResourceCommands;
import org.apache.lens.client.LensClient;

import org.apache.commons.io.FileUtils;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestLensLogResourceCommands extends LensCliApplicationTest {

  private LensClient client = null;
  private LensLogResourceCommands commands = null;

  @BeforeTest
  public void setup() {
    client = new LensClient();
    commands = new LensLogResourceCommands();
    commands.setClient(client);
  }

  @AfterTest
  public void cleanup() {
    if (client != null) {
      client.closeConnection();
    }
  }
  @Test
  public void testLogResourceFileDoesNotExist() throws IOException {
    // check for 404 response
    String response = commands.getLogs("randomFile", null);
    Assert.assertTrue(response.contains("404"));
  }

  @Test
  public void testLogsCommand() throws IOException {
    String request = "testId";
    File file = createFileWithContent(request, "test log resource");

    // create output directory to store the resulted log file
    String outputDirName = "target/sample-logs/";
    File dir = new File(outputDirName);
    dir.mkdirs();

    String response = commands.getLogs(request, outputDirName);
    File outputFile = new File(outputDirName + request);
    Assert.assertTrue(FileUtils.contentEquals(file, outputFile));
    Assert.assertTrue(response.contains("Saved to"));

    response = commands.getLogs(request, null);
    Assert.assertTrue(response.contains("printed complete log content"));

    // check 404 response
    response = commands.getLogs("random", null);
    Assert.assertTrue(response.contains("404"));
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

  private File createFileWithContent(String filename, String content) throws IOException {
    File file = createNewPath("target/" + filename + ".log");
    FileWriter fw = new FileWriter(file.getAbsoluteFile());
    BufferedWriter bw = new BufferedWriter(fw);
    bw.write(content);
    bw.close();
    return file;
  }
}
