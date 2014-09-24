package com.inmobi.grill.lib.query;

/*
 * #%L
 * Grill Query Library
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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.query.PersistedOutputFormatter;

public class TestFilePersistentFormatter extends TestAbstractFileFormatter {

  private Path partFileDir = new Path("file:///tmp/partcsvfiles");
  private Path partFileTextDir = new Path("file:///tmp/parttextfiles");

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
    writer.write("\"NULL\",\"NULL\",\"NULL\",\"NULL\",\"1,2,NULL,4,NULL\",\"NULL:NULL\",\"1=one,2=two,NULL=three,4=NULL,5=NULL\"\n");
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

  @AfterTest
  public void cleanupPartFiles() throws IOException {
    FileSystem fs = partFileDir.getFileSystem(new Configuration());
    fs.delete(partFileDir, true);
    fs.delete(partFileTextDir, true);
  }

  @Override
  protected WrappedFileFormatter createFormatter() {
    return new FilePersistentFormatter();
  }

  @Override
  protected void writeAllRows(Configuration conf) throws IOException {
    ((PersistedOutputFormatter)formatter).addRowsFromPersistedPath(new Path(conf.get("test.partfile.dir")));
  }

  protected void setConf(Configuration conf) {
    conf.set("test.partfile.dir", partFileDir.toString());
    conf.set(GrillConfConstants.QUERY_OUTPUT_HEADER, "\"firstcol\",\"format(secondcol,2)\",\"thirdcol\",\"fourthcol\",\"fifthcol\",\"sixthcol\",\"seventhcol\"");
    conf.set(GrillConfConstants.QUERY_OUTPUT_FOOTER, "Total rows:5");
  }

  @Test
  public void testCSVWithSerdeHeader() throws IOException {
    Configuration conf = new Configuration();
    setConf(conf);
    conf.set(GrillConfConstants.QUERY_OUTPUT_HEADER, "");
    testFormatter(conf, "UTF8",
        GrillConfConstants.GRILL_RESULT_SET_PARENT_DIR_DEFAULT, ".csv",getMockedResultSet());
    // validate rows
    Assert.assertEquals(readFinalOutputFile(
        new Path(formatter.getFinalOutputPath()), conf, "UTF-8"), getExpectedCSVRows());
  }

  @Test
  public void testTextFiles() throws IOException {
    Configuration conf = new Configuration();
    setConf(conf);
    conf.set("test.partfile.dir", partFileTextDir.toString());
    conf.set(GrillConfConstants.QUERY_OUTPUT_FILE_EXTN, ".txt");
    conf.set(GrillConfConstants.QUERY_OUTPUT_HEADER, "firstcolsecondcolthirdcolfourthcolfifthcolsixthcolseventhcol");
    testFormatter(conf, "UTF8",
        GrillConfConstants.GRILL_RESULT_SET_PARENT_DIR_DEFAULT, ".txt",getMockedResultSetWithoutComma());
    // validate rows
    Assert.assertEquals(readFinalOutputFile(
        new Path(formatter.getFinalOutputPath()), conf, "UTF-8"), getExpectedTextRowsWithoutComma());
  }

  @Test
  public void testTextFileWithSerdeHeader() throws IOException {
    Configuration conf = new Configuration();
    setConf(conf);
    conf.set("test.partfile.dir", partFileTextDir.toString());
    conf.set(GrillConfConstants.QUERY_OUTPUT_FILE_EXTN, ".txt");
    conf.set(GrillConfConstants.QUERY_OUTPUT_HEADER, "");
    conf.set(GrillConfConstants.QUERY_OUTPUT_SERDE, LazySimpleSerDe.class.getCanonicalName());
    testFormatter(conf, "UTF8",
        GrillConfConstants.GRILL_RESULT_SET_PARENT_DIR_DEFAULT, ".txt",getMockedResultSetWithoutComma());
    // validate rows
    Assert.assertEquals(readFinalOutputFile(
        new Path(formatter.getFinalOutputPath()), conf, "UTF-8"), getExpectedTextRowsWithoutComma());
  }

  @Test
  public void testTextFilesWithCompression() throws IOException {
    Configuration conf = new Configuration();
    setConf(conf);
    conf.set("test.partfile.dir", partFileTextDir.toString());
    conf.set(GrillConfConstants.QUERY_OUTPUT_FILE_EXTN, ".txt");
    conf.setBoolean(GrillConfConstants.QUERY_OUTPUT_ENABLE_COMPRESSION, true);
    conf.set(GrillConfConstants.QUERY_OUTPUT_HEADER, "firstcolformat(secondcol,2)thirdcolfourthcolfifthcolsixthcolseventhcol");
    testFormatter(conf, "UTF8",
        GrillConfConstants.GRILL_RESULT_SET_PARENT_DIR_DEFAULT, ".txt.gz",getMockedResultSetWithoutComma());
    // validate rows
    Assert.assertEquals(readCompressedFile(
        new Path(formatter.getFinalOutputPath()), conf, "UTF-8"), getExpectedTextRows());
  }

  @Test
  public void testTextFileWithZipFormatter() throws IOException {
    Configuration conf = new Configuration();
    setConf(conf);
    conf.set("test.partfile.dir", partFileTextDir.toString());
    conf.set(GrillConfConstants.QUERY_OUTPUT_FILE_EXTN, ".txt");
    conf.set(GrillConfConstants.QUERY_OUTPUT_HEADER, "");
    conf.set(GrillConfConstants.QUERY_OUTPUT_SERDE, LazySimpleSerDe.class.getCanonicalName());
    conf.setBoolean(GrillConfConstants.RESULT_SPLIT_INTO_MULTIPLE, true);
    conf.setLong(GrillConfConstants.RESULT_SPLIT_MULTIPLE_MAX_ROWS, 2L);
    testFormatter(conf, "UTF8",
        GrillConfConstants.GRILL_RESULT_SET_PARENT_DIR_DEFAULT, ".zip",getMockedResultSetWithoutComma());
    // validate rows
    List<String> actual = readZipOutputFile(
        new Path(formatter.getFinalOutputPath()), conf, "UTF-8");
    System.out.println("Actual rows:" + actual);
    Assert.assertEquals(actual, getExpectedTextRowsWithMultipleWithoutComma());
  }

  @Test
  public void testCSVWithZipFormatter() throws IOException {
    Configuration conf = new Configuration();
    setConf(conf);
    conf.set(GrillConfConstants.QUERY_OUTPUT_HEADER, "");
    conf.setBoolean(GrillConfConstants.RESULT_SPLIT_INTO_MULTIPLE, true);
    conf.setLong(GrillConfConstants.RESULT_SPLIT_MULTIPLE_MAX_ROWS, 2L);
    testFormatter(conf, "UTF8",
        GrillConfConstants.GRILL_RESULT_SET_PARENT_DIR_DEFAULT, ".zip",getMockedResultSet());
    // validate rows
    List<String> actual = readZipOutputFile(
        new Path(formatter.getFinalOutputPath()), conf, "UTF-8");
    System.out.println("Actual rows:" + actual);
    Assert.assertEquals(actual, getExpectedCSVRowsWithMultiple());
  }
}
