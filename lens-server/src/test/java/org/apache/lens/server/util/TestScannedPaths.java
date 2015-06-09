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

package org.apache.lens.server.util;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Iterators;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Test(groups = "unit-test")
public class TestScannedPaths {

  public void testScannedPaths() throws Exception {
    File fileA = null, fileB = null;
    String filenameA, filenameB, fileRegex;
    String tempPath = "target/tempfiles/";
    ScannedPaths sc;
    Iterator<String> iter = null;

    try {
      filenameA = "tempdata_a";
      filenameB = "tempdata_b";
      fileRegex = tempPath + "tempdata_*";

      fileA = createNewPath(tempPath + filenameA);
      fileB = createNewPath(tempPath + filenameB);
      sc = new ScannedPaths(fileRegex, "file");

      Assert.assertEquals(Iterators.size(sc.iterator()), 2, "Incorrect number of matches found");
      iter = sc.iterator();
      Assert.assertTrue(iter.next().contains(filenameA));
      Assert.assertTrue(iter.next().contains(filenameB));

      /**
       * Testing negative Scenario
       **/
      fileRegex = tempPath + "tempdata_unknown_*";
      sc = new ScannedPaths(fileRegex, "file");

      Assert.assertNull(sc.iterator(), "Iterator should be null for unmatched path patterns.");
    } catch (Exception e) {
      Assert.fail("Exception while testing ScannedPaths : " + e.getMessage());
    } finally {
      FileUtils.deleteQuietly(new File(tempPath));
    }
  }

  public void testScannedPathsJarGlobOrder() throws Exception {
    File fileA = null, fileB = null, fileC = null;
    File jarFile = null, globFile = null;
    String filenameA, filenameB, filenameC, fileRegex;
    String tempPath = "target/tempfiles/";
    ScannedPaths sc;
    Iterator<String> iter = null;
    PrintWriter writer;

    try {
      filenameA = "tempdata_a.jar";
      filenameB = "tempdata_b.jar";
      filenameC = "tempdata_c.data";
      fileRegex = tempPath;

      fileA = createNewPath(tempPath + filenameA);
      fileB = createNewPath(tempPath + filenameB);
      fileC = createNewPath(tempPath + filenameC);

      /** Test jar_order **/
      jarFile = createNewPath(tempPath + "jar_order");
      writer = new PrintWriter(jarFile, "UTF-8");
      writer.println(filenameB);
      writer.println(filenameA);
      writer.close();
      sc = new ScannedPaths(fileRegex, "jar");
      Assert.assertEquals(Iterators.size(sc.iterator()), 2, "Incorrect number of matches found");
      iter = sc.iterator();
      Assert.assertTrue(iter.next().contains(filenameB));
      Assert.assertTrue(iter.next().contains(filenameA));

      /** Test glob_order **/
      if (jarFile != null) {
        jarFile.delete();
      }
      globFile = createNewPath(tempPath + "glob_order");
      writer = new PrintWriter(globFile, "UTF-8");
      writer.println(filenameB);
      writer.println(filenameA);
      writer.println(filenameC);
      writer.close();
      sc = new ScannedPaths(fileRegex, "file");
      Assert.assertEquals(Iterators.size(sc.iterator()), 3, "Incorrect number of matches found");
      iter = sc.iterator();
      Assert.assertTrue(iter.next().contains(filenameB));
      Assert.assertTrue(iter.next().contains(filenameA));
      Assert.assertTrue(iter.next().contains(filenameC));
    } catch (Exception e) {
      Assert.fail("Exception while testing ScannedPaths : " + e.getMessage());
    } finally {
      FileUtils.deleteQuietly(new File(tempPath));
    }
  }

  private File createNewPath(String fileName) {
    File f = new File(fileName);
    try {
      if (!f.getParentFile().exists()) {
        f.getParentFile().mkdirs();
      }
      if (!f.exists()) {
        f.createNewFile();
      }
    } catch (IOException e) {
      Assert.fail("Unable to create test file, so bailing out.");
    }
    return f;
  }

}
