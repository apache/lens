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
package org.apache.lens.lib.query;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.query.PersistedOutputFormatter;
import org.apache.lens.server.api.query.QueryContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * The Class TestFilePersistentFormatter.
 */
public class TestFilePersistentFormatter extends TestAbstractFileFormatter {

  /**
   * The part file dir.
   */
  private Path partFileDir = new Path("target/partcsvfiles");

  /**
   * The part file text dir.
   */
  private Path partFileTextDir = new Path("target/parttextfiles");

  /**
   * Creates the part files.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @BeforeTest
  public void createPartFiles() throws IOException {

    // create csv files
    FileSystem fs = partFileDir.getFileSystem(new Configuration());
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(partFileDir, "000000_2"))));
    writer.write("\"1\",\"one\",\"one\",\"one\",\"1\",\"1:one\",\"1=one\"\n");
    writer.close();
    writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(partFileDir, "000001_0"))));
    writer.write("\"2\",\"two\",\"two\",\"two\",\"1,2\",\"2:two\",\"1=one,2=two\"\n");
    writer.write("\"NULL\",\"three\",\"three\",\"three\",\"1,2,NULL\",\"NULL:three\",\"1=one,2=two,NULL=three\"\n");
    writer.close();
    writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(partFileDir, "000010_1"))));
    writer.write("\"4\",\"NULL\",\"NULL\",\"NULL\",\"1,2,NULL,4\",\"4:NULL\",\"1=one,2=two,NULL=three,4=NULL\"\n");
    writer
      .write("\"NULL\",\"NULL\",\"NULL\",\"NULL\",\"1,2,NULL,4,NULL\",\"NULL:NULL\","
        + "\"1=one,2=two,NULL=three,4=NULL,5=NULL\"\n");
    writer.close();
    writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(partFileDir, "_SUCCESS"))));
    writer.close();

    // create text files
    fs = partFileTextDir.getFileSystem(new Configuration());
    writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(partFileTextDir, "000000_2"))));
    writer.write("1oneoneone            11one1one       \n");
    writer.close();
    writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(partFileTextDir, "000001_0"))));
    writer.write("2twotwotwo            122two1one       2two       \n");
    writer.write("\\Nthreethreethree          12\\N\\Nthree1one       2two       \\Nthree     \n");
    writer.close();
    writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(partFileTextDir, "000010_1"))));
    writer.write("4\\N\\N\\N12\\N44\\N1one       2two       \\Nthree     4\\N\n");
    writer.write("\\N\\N\\N\\N12\\N4\\N\\N\\N1one       2two       \\Nthree     4\\N5\\N\n");
    writer.close();
    writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(partFileTextDir, "_SUCCESS"))));
    writer.close();
  }

  /**
   * Cleanup part files.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @AfterTest
  public void cleanupPartFiles() throws IOException {
    FileSystem fs = partFileDir.getFileSystem(new Configuration());
    fs.delete(partFileDir, true);
    fs.delete(partFileTextDir, true);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.lib.query.TestAbstractFileFormatter#createFormatter()
   */
  @Override
  protected WrappedFileFormatter createFormatter() {
    return new FilePersistentFormatter();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.lib.query.TestAbstractFileFormatter#writeAllRows(org.apache.hadoop.conf.Configuration)
   */
  @Override
  protected void writeAllRows(Configuration conf) throws IOException {
    ((PersistedOutputFormatter) formatter).addRowsFromPersistedPath(new Path(conf.get("test.partfile.dir")));
  }

  protected void setConf(Configuration conf) {
    conf.set("test.partfile.dir", partFileDir.toString());
    conf.set(LensConfConstants.QUERY_OUTPUT_HEADER,
      "\"firstcol\",\"format(secondcol,2)\",\"thirdcol\",\"fourthcol\",\"fifthcol\",\"sixthcol\",\"seventhcol\"");
    conf.set(LensConfConstants.QUERY_OUTPUT_FOOTER, "Total rows:5");
  }

  /**
   * Test csv with serde header.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Test
  public void testCSVWithSerdeHeader() throws IOException {
    Configuration conf = new Configuration();
    setConf(conf);
    conf.set(LensConfConstants.QUERY_OUTPUT_HEADER, "");
    testFormatter(conf, "UTF8", LensConfConstants.RESULT_SET_PARENT_DIR_DEFAULT, ".csv", getMockedResultSet());
    // validate rows
    Assert.assertEquals(readFinalOutputFile(new Path(formatter.getFinalOutputPath()), conf, "UTF-8"),
      getExpectedCSVRows());
  }

  /**
   * Test text files.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Test
  public void testTextFiles() throws IOException {
    Configuration conf = new Configuration();
    setConf(conf);
    conf.set("test.partfile.dir", partFileTextDir.toString());
    conf.set(LensConfConstants.QUERY_OUTPUT_FILE_EXTN, ".txt");
    conf.set(LensConfConstants.QUERY_OUTPUT_HEADER,
      "firstcolsecondcolthirdcolfourthcolfifthcolsixthcolseventhcol");
    testFormatter(conf, "UTF8", LensConfConstants.RESULT_SET_PARENT_DIR_DEFAULT, ".txt",
      getMockedResultSetWithoutComma());
    // validate rows
    Assert.assertEquals(readFinalOutputFile(new Path(formatter.getFinalOutputPath()), conf, "UTF-8"),
      getExpectedTextRowsWithoutComma());
  }

  /**
   * Test text file with serde header.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Test
  public void testTextFileWithSerdeHeader() throws IOException {
    Configuration conf = new Configuration();
    setConf(conf);
    conf.set("test.partfile.dir", partFileTextDir.toString());
    conf.set(LensConfConstants.QUERY_OUTPUT_FILE_EXTN, ".txt");
    conf.set(LensConfConstants.QUERY_OUTPUT_HEADER, "");
    conf.set(LensConfConstants.QUERY_OUTPUT_SERDE, LazySimpleSerDe.class.getCanonicalName());
    testFormatter(conf, "UTF8", LensConfConstants.RESULT_SET_PARENT_DIR_DEFAULT, ".txt",
      getMockedResultSetWithoutComma());
    // validate rows
    Assert.assertEquals(readFinalOutputFile(new Path(formatter.getFinalOutputPath()), conf, "UTF-8"),
      getExpectedTextRowsWithoutComma());
  }

  /**
   * Test text files with compression.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Test
  public void testTextFilesWithCompression() throws IOException {
    Configuration conf = new Configuration();
    setConf(conf);
    conf.set("test.partfile.dir", partFileTextDir.toString());
    conf.set(LensConfConstants.QUERY_OUTPUT_FILE_EXTN, ".txt");
    conf.setBoolean(LensConfConstants.QUERY_OUTPUT_ENABLE_COMPRESSION, true);
    conf.set(LensConfConstants.QUERY_OUTPUT_HEADER,
      "firstcolformat(secondcol,2)thirdcolfourthcolfifthcolsixthcolseventhcol");
    testFormatter(conf, "UTF8", LensConfConstants.RESULT_SET_PARENT_DIR_DEFAULT, ".txt.gz",
      getMockedResultSetWithoutComma());
    // validate rows
    Assert.assertEquals(readCompressedFile(new Path(formatter.getFinalOutputPath()), conf, "UTF-8"),
      getExpectedTextRows());
  }

  /**
   * Test text file with zip formatter.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Test
  public void testTextFileWithZipFormatter() throws IOException {
    Configuration conf = new Configuration();
    setConf(conf);
    conf.set("test.partfile.dir", partFileTextDir.toString());
    conf.set(LensConfConstants.QUERY_OUTPUT_FILE_EXTN, ".txt");
    conf.set(LensConfConstants.QUERY_OUTPUT_HEADER, "");
    conf.set(LensConfConstants.QUERY_OUTPUT_SERDE, LazySimpleSerDe.class.getCanonicalName());
    conf.setBoolean(LensConfConstants.RESULT_SPLIT_INTO_MULTIPLE, true);
    conf.setLong(LensConfConstants.RESULT_SPLIT_MULTIPLE_MAX_ROWS, 2L);
    testFormatter(conf, "UTF8", LensConfConstants.RESULT_SET_PARENT_DIR_DEFAULT, ".zip",
      getMockedResultSetWithoutComma());
    // validate rows
    List<String> actual = readZipOutputFile(new Path(formatter.getFinalOutputPath()), conf, "UTF-8");
    System.out.println("Actual rows:" + actual);
    Assert.assertEquals(actual, getExpectedTextRowsWithMultipleWithoutComma());
  }

  /**
   * Test csv with zip formatter.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Test
  public void testCSVWithZipFormatter() throws IOException {
    Configuration conf = new Configuration();
    setConf(conf);
    conf.set(LensConfConstants.QUERY_OUTPUT_HEADER, "");
    conf.setBoolean(LensConfConstants.RESULT_SPLIT_INTO_MULTIPLE, true);
    conf.setLong(LensConfConstants.RESULT_SPLIT_MULTIPLE_MAX_ROWS, 2L);
    testFormatter(conf, "UTF8", LensConfConstants.RESULT_SET_PARENT_DIR_DEFAULT, ".zip", getMockedResultSet());
    // validate rows
    List<String> actual = readZipOutputFile(new Path(formatter.getFinalOutputPath()), conf, "UTF-8");
    System.out.println("Actual rows:" + actual);
    Assert.assertEquals(actual, getExpectedCSVRowsWithMultiple());
  }

  /**
   * Test text files output path.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Test
  public void testTextFileOutputPath() throws IOException {
    Configuration conf = new Configuration();
    setConf(conf);
    conf.set("test.partfile.dir", partFileTextDir.toString());
    conf.set(LensConfConstants.QUERY_OUTPUT_FILE_EXTN, ".txt");
    conf.set(LensConfConstants.QUERY_OUTPUT_HEADER,
      "firstcolsecondcolthirdcolfourthcolfifthcolsixthcolseventhcol");
    QueryContext ctx = createContext(conf, "test.Query_1 name");
    Path expectedFinalPath = new Path(LensConfConstants.RESULT_SET_PARENT_DIR_DEFAULT,
      "test.Query_1_name-" + ctx.getQueryHandle() + ".txt");
    FileSystem fs = expectedFinalPath.getFileSystem(conf);
    expectedFinalPath = expectedFinalPath.makeQualified(fs);
    validateFormatter(conf, "UTF8", LensConfConstants.RESULT_SET_PARENT_DIR_DEFAULT, ".txt",
      getMockedResultSetWithoutComma(), ctx, expectedFinalPath);
  }

  /**
   * Test text files with a long output path.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Test
  public void testTextFileLongOutputPath() throws IOException {
    Configuration conf = new Configuration();
    setConf(conf);
    conf.set("test.partfile.dir", partFileTextDir.toString());
    conf.set(LensConfConstants.QUERY_OUTPUT_FILE_EXTN, ".txt");
    conf.set(LensConfConstants.QUERY_OUTPUT_HEADER,
      "firstcolsecondcolthirdcolfourthcolfifthcolsixthcolseventhcol");
    QueryContext ctx = createContext(conf, "test-Query 1^name12345678901234567890123456789012345678901234567890");
    Path expectedFinalPath = new Path(LensConfConstants.RESULT_SET_PARENT_DIR_DEFAULT,
      "test-Query_1_name123456789012345678901234567890123-" + ctx.getQueryHandle() + ".txt");
    FileSystem fs = expectedFinalPath.getFileSystem(conf);
    expectedFinalPath = expectedFinalPath.makeQualified(fs);
    validateFormatter(conf, "UTF8", LensConfConstants.RESULT_SET_PARENT_DIR_DEFAULT, ".txt",
      getMockedResultSetWithoutComma(), ctx, expectedFinalPath);
  }

  /**
   * Test zip csv files output path.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Test
  public void testCSVZipFileOutputPath() throws IOException {
    Configuration conf = new Configuration();
    setConf(conf);
    conf.setBoolean(LensConfConstants.RESULT_SPLIT_INTO_MULTIPLE, true);
    conf.setLong(LensConfConstants.RESULT_SPLIT_MULTIPLE_MAX_ROWS, 2L);
    QueryContext ctx = createContext(conf, "Test.query_1 name");
    Path expectedFinalPath = new Path(LensConfConstants.RESULT_SET_PARENT_DIR_DEFAULT,
      "Test.query_1_name-" + ctx.getQueryHandle() + ".zip");
    FileSystem fs = expectedFinalPath.getFileSystem(conf);
    expectedFinalPath = expectedFinalPath.makeQualified(fs);
    validateFormatter(conf, "UTF8", LensConfConstants.RESULT_SET_PARENT_DIR_DEFAULT, ".zip",
      getMockedResultSetWithoutComma(), ctx, expectedFinalPath);
    ZipEntry ze = null;
    ZipInputStream zin = new ZipInputStream(fs.open(expectedFinalPath));
    int i = 0;
    while ((ze = zin.getNextEntry()) != null) {
      Assert.assertEquals(ze.getName(), "Test.query_1_name-" + ctx.getQueryHandle() + "_part-" + i + ".csv");
      i++;
      zin.closeEntry();
    }
    zin.close();
  }

  protected List<String> getExpectedCSVRows() {
    return new ArrayList<String>() {
      {
        add("\"firstcol\",\"format(secondcol,2)\",\"thirdcol\",\"fourthcol\",\"fifthcol\",\"sixthcol\",\"seventhcol\"");
        add("\"1\",\"one\",\"one\",\"one\",\"1\",\"1:one\",\"1=one\"");
        add("\"2\",\"two\",\"two\",\"two\",\"1,2\",\"2:two\",\"1=one,2=two\"");
        add("\"NULL\",\"three\",\"three\",\"three\",\"1,2,NULL\",\"NULL:three\",\"1=one,2=two,NULL=three\"");
        add("\"4\",\"NULL\",\"NULL\",\"NULL\",\"1,2,NULL,4\",\"4:NULL\",\"1=one,2=two,NULL=three,4=NULL\"");
        add("\"NULL\",\"NULL\",\"NULL\",\"NULL\",\"1,2,NULL,4,NULL\",\"NULL:NULL\","
          + "\"1=one,2=two,NULL=three,4=NULL,5=NULL\"");
        add("Total rows:5");
      }
    };
  }

  protected List<String> getExpectedTextRows() {
    return new ArrayList<String>() {
      {
        add("firstcolformat(secondcol,2)thirdcolfourthcolfifthcolsixthcolseventhcol");
        add("1oneoneone            11one1one       ");
        add("2twotwotwo            122two1one       2two       ");
        add("\\Nthreethreethree          12\\N\\Nthree1one       2two       \\Nthree     ");
        add("4\\N\\N\\N12\\N44\\N1one       2two       \\Nthree     4\\N");
        add("\\N\\N\\N\\N12\\N4\\N\\N\\N1one       2two       \\Nthree     4\\N5\\N");
        add("Total rows:5");
      }
    };
  }

  protected List<String> getExpectedCSVRowsWithoutComma() {
    return new ArrayList<String>() {
      {
        add("\"firstcol\",\"secondcol\",\"thirdcol\",\"fourthcol\",\"fifthcol\",\"sixthcol\",\"seventhcol\"");
        add("\"1\",\"one\",\"one\",\"one\",\"1\",\"1:one\",\"1=one\"");
        add("\"2\",\"two\",\"two\",\"two\",\"1,2\",\"2:two\",\"1=one,2=two\"");
        add("\"NULL\",\"three\",\"three\",\"three\",\"1,2,NULL\",\"NULL:three\",\"1=one,2=two,NULL=three\"");
        add("\"4\",\"NULL\",\"NULL\",\"NULL\",\"1,2,NULL,4\",\"4:NULL\",\"1=one,2=two,NULL=three,4=NULL\"");
        add("\"NULL\",\"NULL\",\"NULL\",\"NULL\",\"1,2,NULL,4,NULL\",\"NULL:NULL\","
          + "\"1=one,2=two,NULL=three,4=NULL,5=NULL\"");
        add("Total rows:5");
      }
    };
  }

  protected List<String> getExpectedTextRowsWithoutComma() {
    return new ArrayList<String>() {
      {
        add("firstcolsecondcolthirdcolfourthcolfifthcolsixthcolseventhcol");
        add("1oneoneone            11one1one       ");
        add("2twotwotwo            122two1one       2two       ");
        add("\\Nthreethreethree          12\\N\\Nthree1one       2two       \\Nthree     ");
        add("4\\N\\N\\N12\\N44\\N1one       2two       \\Nthree     4\\N");
        add("\\N\\N\\N\\N12\\N4\\N\\N\\N1one       2two       \\Nthree     4\\N5\\N");
        add("Total rows:5");
      }
    };
  }

  protected List<String> getExpectedCSVRowsWithMultiple() {
    return new ArrayList<String>() {
      {

        add("\"firstcol\",\"format(secondcol,2)\",\"thirdcol\",\"fourthcol\",\"fifthcol\",\"sixthcol\",\"seventhcol\"");
        add("\"1\",\"one\",\"one\",\"one\",\"1\",\"1:one\",\"1=one\"");
        add("\"2\",\"two\",\"two\",\"two\",\"1,2\",\"2:two\",\"1=one,2=two\"");
        add("\"firstcol\",\"format(secondcol,2)\",\"thirdcol\",\"fourthcol\",\"fifthcol\",\"sixthcol\",\"seventhcol\"");
        add("\"NULL\",\"three\",\"three\",\"three\",\"1,2,NULL\",\"NULL:three\",\"1=one,2=two,NULL=three\"");
        add("\"4\",\"NULL\",\"NULL\",\"NULL\",\"1,2,NULL,4\",\"4:NULL\",\"1=one,2=two,NULL=three,4=NULL\"");
        add("\"firstcol\",\"format(secondcol,2)\",\"thirdcol\",\"fourthcol\",\"fifthcol\",\"sixthcol\",\"seventhcol\"");
        add("\"NULL\",\"NULL\",\"NULL\",\"NULL\",\"1,2,NULL,4,NULL\",\"NULL:NULL\","
          + "\"1=one,2=two,NULL=three,4=NULL,5=NULL\"");
        add("Total rows:5");
      }
    };
  }

  protected List<String> getExpectedTextRowsWithMultiple() {
    return new ArrayList<String>() {
      {
        add("firstcolformat(secondcol,2)thirdcolfourthcolfifthcolsixthcolseventhcol");
        add("1oneoneone            11one1one       ");
        add("2twotwotwo            122two1one       2two       ");
        add("firstcolformat(secondcol,2)thirdcolfourthcolfifthcolsixthcolseventhcol");
        add("\\Nthreethreethree          12\\N\\Nthree1one       2two       \\Nthree     ");
        add("4\\N\\N\\N12\\N44\\N1one       2two       \\Nthree     4\\N");
        add("firstcolformat(secondcol,2)thirdcolfourthcolfifthcolsixthcolseventhcol");
        add("\\N\\N\\N\\N12\\N4\\N\\N\\N1one       2two       \\Nthree     4\\N5\\N");
        add("Total rows:5");
      }
    };
  }

  protected List<String> getExpectedCSVRowsWithMultipleWithoutComma() {
    return new ArrayList<String>() {
      {
        add("\"firstcol\",\"secondcol\",\"thirdcol\",\"fourthcol\",\"fifthcol\",\"sixthcol\",\"seventhcol\"");
        add("\"1\",\"one\",\"one\",\"one\",\"1\",\"1:one\",\"1=one\"");
        add("\"2\",\"two\",\"two\",\"two\",\"1,2\",\"2:two\",\"1=one,2=two\"");
        add("\"firstcol\",\"secondcol\",\"thirdcol\",\"fourthcol\",\"fifthcol\",\"sixthcol\",\"seventhcol\"");
        add("\"NULL\",\"three\",\"three\",\"three\",\"1,2,NULL\",\"NULL:three\",\"1=one,2=two,NULL=three\"");
        add("\"4\",\"NULL\",\"NULL\",\"NULL\",\"1,2,NULL,4\",\"4:NULL\",\"1=one,2=two,NULL=three,4=NULL\"");
        add("\"firstcol\",\"secondcol\",\"thirdcol\",\"fourthcol\",\"fifthcol\",\"sixthcol\",\"seventhcol\"");
        add("\"NULL\",\"NULL\",\"NULL\",\"NULL\",\"1,2,NULL,4,NULL\",\"NULL:NULL\","
          + "\"1=one,2=two,NULL=three,4=NULL,5=NULL\"");
        add("Total rows:5");
      }
    };
  }

  protected List<String> getExpectedTextRowsWithMultipleWithoutComma() {
    return new ArrayList<String>() {
      {
        add("firstcolsecondcolthirdcolfourthcolfifthcolsixthcolseventhcol");
        add("1oneoneone            11one1one       ");
        add("2twotwotwo            122two1one       2two       ");
        add("firstcolsecondcolthirdcolfourthcolfifthcolsixthcolseventhcol");
        add("\\Nthreethreethree          12\\N\\Nthree1one       2two       \\Nthree     ");
        add("4\\N\\N\\N12\\N44\\N1one       2two       \\Nthree     4\\N");
        add("firstcolsecondcolthirdcolfourthcolfifthcolsixthcolseventhcol");
        add("\\N\\N\\N\\N12\\N4\\N\\N\\N1one       2two       \\Nthree     4\\N5\\N");
        add("Total rows:5");
      }
    };
  }
}
