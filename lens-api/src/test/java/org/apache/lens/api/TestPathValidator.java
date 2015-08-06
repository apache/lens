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
package org.apache.lens.api;

import java.io.File;
import java.io.IOException;

import org.apache.lens.api.util.PathValidator;

import org.junit.After;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestPathValidator {
  private String[] inputPaths = {
    "/tmp/lens/server",
    "file:///tmp/lens/server",
    "file:////tmp/lens/server",
    "hdfs:/tmp/lens/server",
    "hdfs:///tmp/lens/server",
    "~/tmp/lens/server",
  };

  private String [] validPaths = {
    "/tmp/lens/server",
    "file:///tmp/lens/server",
    "file:///tmp/lens/server",
    "hdfs://tmp/lens/server",
    "hdfs://tmp/lens/server",
    System.getProperty("user.home") + "/tmp/lens/server",
  };

  private static final String PROJECT_DIR = new File("").getAbsolutePath();
  private static final String EXISTING_PATH = PROJECT_DIR + "/target/tmp/lens/server";
  private static final String NON_EXISTING_PATH = PROJECT_DIR + "/target/tmp/lens/dir";

  private static final File EXISTING_FILE = new File(EXISTING_PATH);
  private static final File NON_EXISTING_FILE = new File(NON_EXISTING_PATH);


  @BeforeTest
  public void beforeTest() {
    cleanupFiles();
  }

  @After
  public void after() {
    cleanupFiles();
  }

  private void cleanupFiles() {
    try {
      if (EXISTING_FILE.exists()) {
        EXISTING_FILE.delete();
      }
      if (NON_EXISTING_FILE.exists()) {
        NON_EXISTING_FILE.delete();
      }
    } catch (Exception ex) {
      Assert.fail("Unable to delete file.", ex);
    }
  }

  @Test
  public void testExistingFileWithoutChecks() {
    cleanupFiles();
    /** Test: without checks **/
    PathValidator validator = new PathValidator(new LensConf());
    Assert.assertEquals(validator.getValidPath(EXISTING_FILE, false, false), EXISTING_PATH);
  }

  @Test
  public void testExistingFileShouldExist() throws IOException{
    PathValidator validator = new PathValidator(new LensConf());
    /** Test: file should exist **/
    if (!EXISTING_FILE.exists()) {
      EXISTING_FILE.getParentFile().mkdirs();
      EXISTING_FILE.createNewFile();
    }
    Assert.assertEquals(validator.getValidPath(EXISTING_FILE, false, true), EXISTING_PATH);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testNonExistingFileShouldHadExisted() {
    /** Test: non existing file should had existed  **/
    /* Should throw RuntimeException */
    PathValidator validator = new PathValidator(new LensConf());
    validator.getValidPath(NON_EXISTING_FILE, true, true);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testNonExistingFileShouldHadBeenDir() {
    /** Test: non existing file should had been dir  **/
    /* Should throw RuntimeException */
    PathValidator validator = new PathValidator(new LensConf());
    validator.getValidPath(NON_EXISTING_FILE, true, false);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testDirShouldHadBeenFile() {
    /** Test: dir should had been file **/
    if (!NON_EXISTING_FILE.exists()) {
      NON_EXISTING_FILE.mkdirs();
    }
    /* Should throw RuntimeException */
    PathValidator validator = new PathValidator(new LensConf());
    validator.getValidPath(NON_EXISTING_FILE, false, false);
  }

  @Test
  public void testRemovePrefixBeforeURI() {
    PathValidator validator = new PathValidator(new LensConf());
    for (int index = 0; index < inputPaths.length; index++) {
      Assert.assertEquals(
          validator.removePrefixBeforeURI(inputPaths[index]),
          validPaths[index],
          "Test failed for input " + inputPaths[index] + " [index:" + index + "]");
    }
  }
}
