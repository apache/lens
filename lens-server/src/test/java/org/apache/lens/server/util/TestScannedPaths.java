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

import com.google.common.base.Joiner;
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

      Assert.assertFalse(sc.iterator().hasNext(), "Iterator should be empty for unmatched path patterns.");
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

  /**
   * Private method used by testScannedPathsMultipleJarGlobOrder.
   * Creates temp dir structure with files.
   *
   * Dir structure created:
   * sourceDirPath/tempfiles/tempdata_a.jar
   * sourceDirPath/tempfiles/tempdata_c.jar
   * sourceDirPath/tempfiles/dir1/tempdata_a.jar
   * sourceDirPath/tempfiles/dir1/tempdata_b.jar
   * sourceDirPath/tempfiles/dir1/tempdata_c.data
   * sourceDirPath/tempfiles/dir2/tempdata_a.data
   * sourceDirPath/tempfiles/dir2/tempdata_b.data
   * sourceDirPath/tempfiles/dir2/tempdata_c.jar
   * sourceDirPath/tempfiles/dir3/tempdata_a.jar
   * sourceDirPath/tempfiles/dir3/tempdata_b.data
   * sourceDirPath/tempfiles/dir3/tempdata_c-duplicate.jar
   * sourceDirPath/tempfiles/dir3/tempdata_c.jar
   *
   * // Additional regex checks for regex in jar_order/glob_order
   * sourceDirPath/tempfiles/dir3/innerDirA/inceptionLevel2/tempdata_c.jar
   * sourceDirPath/tempfiles/dir3/innerDirA/inceptionLevel2/tempdata_c-duplicate.jar
   * sourceDirPath/tempfiles/dir3/innerDirB/inceptionLevel2/tempdata_c-duplicate-2.jar
   * @param sourceDirPath
   */
  private void createTempDirStructure1(String sourceDirPath) {
    File jarFile = null, globFile = null;
    String filenameA, filenameB, filenameC;
    String tempPath = sourceDirPath + "/tempfiles/";
    FileUtils.deleteQuietly(new File(tempPath));

    String jarExtension = ".jar";
    String dataExtension = ".data";
    String dir1 = "dir1/", dir2 = "dir2/", dir3 = "dir3/";

    filenameA = "tempdata_a";
    filenameB = "tempdata_b";
    filenameC = "tempdata_c";

    PrintWriter writer;

    try {
      createNewPath(tempPath, filenameC, jarExtension);
      createNewPath(tempPath, filenameA, jarExtension);
      createNewPath(tempPath, dir1, filenameA, jarExtension);
      createNewPath(tempPath, dir1, filenameB, jarExtension);
      createNewPath(tempPath, dir1, filenameC, dataExtension);
      createNewPath(tempPath, dir2, filenameA, dataExtension);
      createNewPath(tempPath, dir2, filenameB, dataExtension);
      createNewPath(tempPath, dir2, filenameC, jarExtension);
      createNewPath(tempPath, dir3, filenameA, jarExtension);
      createNewPath(tempPath, dir3, filenameB, dataExtension);
      createNewPath(tempPath, dir3, filenameC, jarExtension);
      createNewPath(tempPath, dir3, filenameC, "-duplicate", jarExtension);

      /** Additional paths for inner dirs **/
      createNewPath(tempPath, dir3, "innerDirA/inceptionLevel2/", filenameC, jarExtension);
      createNewPath(tempPath, dir3, "innerDirA/inceptionLevel2/", filenameC, "-duplicate", jarExtension);
      createNewPath(tempPath, dir3, "innerDirB/inceptionLevel2/", filenameC, "-duplicate-2", jarExtension);

      /** Create jar_order **/
      jarFile = createNewPath(tempPath, dir1, "jar_order");
      writer = new PrintWriter(jarFile, "UTF-8");
      writer.println(filenameB + jarExtension);
      writer.println(filenameA + jarExtension);
      writer.close();
      jarFile = createNewPath(tempPath, dir2, "jar_order");
      writer = new PrintWriter(jarFile, "UTF-8");
      writer.println(filenameC + jarExtension);
      writer.close();
      jarFile = createNewPath(tempPath, dir3, "jar_order");
      writer = new PrintWriter(jarFile, "UTF-8");
      writer.println(filenameC + "-duplicate" + jarExtension);
      writer.println(filenameA + jarExtension);
      writer.close();

      /** Create glob_order **/
      globFile = createNewPath(tempPath, dir1, "glob_order");
      writer = new PrintWriter(globFile, "UTF-8");
      writer.println(filenameC + dataExtension);
      writer.println(filenameB + jarExtension);
      writer.println(filenameA + jarExtension);
      writer.close();

      globFile = createNewPath(tempPath, dir3, "glob_order");
      writer = new PrintWriter(globFile, "UTF-8");
      writer.println(filenameC + jarExtension);
      writer.println(filenameC + "-duplicate" + jarExtension);
      /** Check regex compatibility in order files **/
      writer.println("inner*/*Level*/" + filenameC + "-duplicate*" + jarExtension);
      writer.close();



    } catch (Exception ex) {
      Assert.fail("Exception while creating temp paths : " + ex.getMessage());
    }
  }


  /**
   * Private method used by testScannedPathsRecursive.
   * Creates temp dir structure with files.
   *
   * Dir structure created:
   * sourceDirPath/tempfiles/a_dir/jar_1
   * sourceDirPath/tempfiles/a_dir/jar_2
   * sourceDirPath/tempfiles/a_dir/jar_order (*1\n*2)
   *
   * sourceDirPath/tempfiles/b_dir/jar_3
   * sourceDirPath/tempfiles/b_dir/jar_4
   * sourceDirPath/tempfiles/b_dir/glob_order (*4\n*3)
   *
   * sourceDirPath/tempfiles/c_dir/jar_5
   * sourceDirPath/tempfiles/c_dir/jar_6
   *
   * sourceDirPath/tempfiles/jar_order (a*\nb*\nc*)
   *
   * @param sourceDirPath
   */
  private void createTempDirStructure2(String sourceDirPath) {
    File orderFile = null;
    String filenameA, filenameB, filenameC;
    String tempPath = sourceDirPath + "/parent_dir/";
    FileUtils.deleteQuietly(new File(tempPath));

    PrintWriter writer;

    try {
      createNewPath(tempPath, "a_dir/jar_1");
      createNewPath(tempPath, "a_dir/jar_2");
      createNewPath(tempPath, "b_dir/jar_3");
      createNewPath(tempPath, "b_dir/jar_4");
      createNewPath(tempPath, "c_dir/jar_5");
      createNewPath(tempPath, "c_dir/jar_6");


      /** Create jar_order **/
      orderFile = createNewPath(tempPath, "a_dir/jar_order");
      writer = new PrintWriter(orderFile, "UTF-8");
      writer.println("*1");
      writer.println("*2");
      writer.close();

      orderFile = createNewPath(tempPath, "b_dir/glob_order");
      writer = new PrintWriter(orderFile, "UTF-8");
      writer.println("*4");
      writer.println("*3");
      writer.close();

      orderFile = createNewPath(tempPath, "jar_order");
      writer = new PrintWriter(orderFile, "UTF-8");
      writer.println("a*");
      writer.println("b*");
      writer.println("c*");
      writer.close();

    } catch (Exception ex) {
      Assert.fail("Exception while creating temp paths : " + ex.getMessage());
    }
  }

  public void testScannedPathsMultipleJarGlobOrder() throws Exception {
    ScannedPaths sc;
    Iterator<String> iter = null;
    String tempPath = "target/test/";

    String filenameA = "tempdata_a";
    String filenameB = "tempdata_b";
    String filenameC = "tempdata_c";

    String fileRegex1 = tempPath + "*";
    String fileRegex2 = tempPath + "*/*";

    try {
      createTempDirStructure1(tempPath);

      sc = new ScannedPaths(fileRegex1, "jar");
      Assert.assertEquals(Iterators.size(sc.iterator()), 2, "Incorrect number of matches found");
      iter = sc.iterator();
      Assert.assertTrue(iter.next().contains(filenameC));
      Assert.assertTrue(iter.next().contains(filenameA));

      sc = new ScannedPaths(fileRegex2, "jar");
      Assert.assertEquals(Iterators.size(sc.iterator()), 7, "Incorrect number of matches found");
      iter = sc.iterator();
      Assert.assertTrue(iter.next().contains(filenameB));
      Assert.assertTrue(iter.next().contains(filenameA));
      Assert.assertTrue(iter.next().contains(filenameC)); // Direct matched tempdata_c.jar
      Assert.assertTrue(iter.next().contains(filenameC));
      Assert.assertTrue(iter.next().contains(filenameC + "-duplicate"));
      Assert.assertTrue(iter.next().contains(filenameA));
      Assert.assertTrue(iter.next().contains(filenameA)); // Direct matched tempdata_a.jar

      /** Remove jar_order files from temp dir **/
      FileUtils.deleteQuietly(new File(tempPath + "tempfiles/dir1/jar_order"));
      FileUtils.deleteQuietly(new File(tempPath + "tempfiles/dir2/jar_order"));
      FileUtils.deleteQuietly(new File(tempPath + "tempfiles/dir3/jar_order"));

      sc = new ScannedPaths(fileRegex1, "file");
      Assert.assertEquals(Iterators.size(sc.iterator()), 2, "Incorrect number of matches found");
      iter = sc.iterator();
      Assert.assertTrue(iter.next().contains(filenameC));
      Assert.assertTrue(iter.next().contains(filenameA));

      sc = new ScannedPaths(fileRegex2, "file");
      Assert.assertEquals(Iterators.size(sc.iterator()), 12, "Incorrect number of matches found");
      iter = sc.iterator();
      Assert.assertTrue(iter.next().contains(filenameC));
      Assert.assertTrue(iter.next().contains(filenameB));
      Assert.assertTrue(iter.next().contains(filenameA));
      Assert.assertTrue(iter.next().contains(filenameC)); // Direct matched tempdata_c.jar
      Assert.assertTrue(iter.next().contains(filenameC));
      Assert.assertTrue(iter.next().contains(filenameB));
      Assert.assertTrue(iter.next().contains(filenameA));
      Assert.assertTrue(iter.next().contains(filenameC));
      Assert.assertTrue(iter.next().contains(filenameC + "-duplicate"));
      Assert.assertTrue(iter.next().contains(filenameC + "-duplicate-2")); // Inner dir regex match
      Assert.assertTrue(iter.next().contains(filenameC + "-duplicate")); // Inner dir regex match
      Assert.assertTrue(iter.next().contains(filenameA)); // Direct matched tempdata_a.jar

    } catch (Exception e) {
      Assert.fail("Exception while testing ScannedPaths : " + e.getMessage());
    } finally {
      FileUtils.deleteQuietly(new File(tempPath));
    }
  }


  public void testScannedPathsRecursive() throws Exception {
    ScannedPaths sc;
    Iterator<String> iter = null;
    String tempPath = "target/test/";

    String fileRegex = tempPath + "parent_*";

    try {
      createTempDirStructure2(tempPath);

      sc = new ScannedPaths(fileRegex, "file");
      Assert.assertEquals(Iterators.size(sc.iterator()), 6, "Incorrect number of matches found");
      iter = sc.iterator();
      Assert.assertTrue(iter.next().contains("jar_1"));
      Assert.assertTrue(iter.next().contains("jar_2"));
      Assert.assertTrue(iter.next().contains("jar_4"));
      Assert.assertTrue(iter.next().contains("jar_3"));
      Assert.assertTrue(iter.next().contains("jar_6"));
      Assert.assertTrue(iter.next().contains("jar_5"));

      /** Now also enforce order for c_dir **/
      File orderFile = createNewPath(tempPath, "parent_dir/c_dir/glob_order");
      PrintWriter writer = new PrintWriter(orderFile, "UTF-8");
      writer.println("*5");
      writer.println("*6");
      writer.close();

      sc = new ScannedPaths(fileRegex, "file");
      Assert.assertEquals(Iterators.size(sc.iterator()), 6, "Incorrect number of matches found");
      iter = sc.iterator();
      Assert.assertTrue(iter.next().contains("jar_1"));
      Assert.assertTrue(iter.next().contains("jar_2"));
      Assert.assertTrue(iter.next().contains("jar_4"));
      Assert.assertTrue(iter.next().contains("jar_3"));
      Assert.assertTrue(iter.next().contains("jar_5"));
      Assert.assertTrue(iter.next().contains("jar_6"));

    } catch (Exception e) {
      Assert.fail("Exception while testing ScannedPaths : " + e.getMessage());
    } finally {
      FileUtils.deleteQuietly(new File(tempPath));
    }
  }


  public void testScannedPathsNonExistent() throws Exception {
    ScannedPaths sc;
    Iterator<String> iter = null;
    String tempPath = "target/test/";

    String fileRegex = tempPath + "paradox/nopath";

    try {
      sc = new ScannedPaths(fileRegex, "file");
      Assert.assertFalse(sc.iterator().hasNext(), "Iterator should be empty for unmatched path patterns.");

      sc = new ScannedPaths(fileRegex, "jar");
      Assert.assertFalse(sc.iterator().hasNext(), "Iterator should be empty for unmatched path patterns.");

    } catch (Exception e) {
      Assert.fail("Exception while testing ScannedPaths : " + e.getMessage());
    } finally {
      FileUtils.deleteQuietly(new File(tempPath));
    }
  }

  private File createNewPath(String ... params) {
    String fileName = Joiner.on("").join(params);

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
