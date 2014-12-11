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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.lens.api.LensException;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.driver.LensResultSetMetadata;
import org.apache.lens.server.api.driver.MockDriver;
import org.apache.lens.server.api.query.QueryContext;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * The Class TestAbstractFileFormatter.
 */
public abstract class TestAbstractFileFormatter {

  /**
   * The formatter.
   */
  protected WrappedFileFormatter formatter;

  /**
   * Cleanup.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @AfterMethod
  public void cleanup() throws IOException {
    if (formatter != null) {
      FileSystem fs = new Path(formatter.getFinalOutputPath()).getFileSystem(new Configuration());
      fs.delete(new Path(formatter.getFinalOutputPath()), true);
    }
  }

  /**
   * Test formatter.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Test
  public void testFormatter() throws IOException {
    Configuration conf = new Configuration();
    setConf(conf);
    testFormatter(conf, "UTF8", LensConfConstants.RESULT_SET_PARENT_DIR_DEFAULT, ".csv", getMockedResultSet());
    // validate rows
    Assert.assertEquals(readFinalOutputFile(new Path(formatter.getFinalOutputPath()), conf, "UTF-8"),
      getExpectedCSVRows());
  }

  /**
   * Test compression.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Test
  public void testCompression() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(LensConfConstants.QUERY_OUTPUT_ENABLE_COMPRESSION, true);
    setConf(conf);
    testFormatter(conf, "UTF8", LensConfConstants.RESULT_SET_PARENT_DIR_DEFAULT, ".csv.gz", getMockedResultSet());
    // validate rows
    Assert.assertEquals(readCompressedFile(new Path(formatter.getFinalOutputPath()), conf, "UTF-8"),
      getExpectedCSVRows());
  }

  /**
   * Test custom compression.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Test
  public void testCustomCompression() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(LensConfConstants.QUERY_OUTPUT_ENABLE_COMPRESSION, true);
    conf.set(LensConfConstants.QUERY_OUTPUT_COMPRESSION_CODEC,
      org.apache.hadoop.io.compress.DefaultCodec.class.getCanonicalName());
    setConf(conf);
    testFormatter(conf, "UTF8", LensConfConstants.RESULT_SET_PARENT_DIR_DEFAULT, ".csv.deflate", getMockedResultSet());
    // validate rows
    Assert.assertEquals(readCompressedFile(new Path(formatter.getFinalOutputPath()), conf, "UTF-8"),
      getExpectedCSVRows());
  }

  /**
   * Test encoding.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Test
  public void testEncoding() throws IOException {
    Configuration conf = new Configuration();
    conf.set(LensConfConstants.QUERY_OUTPUT_CHARSET_ENCODING, "UTF-16LE");
    setConf(conf);
    testFormatter(conf, "UnicodeLittleUnmarked", LensConfConstants.RESULT_SET_PARENT_DIR_DEFAULT, ".csv",
      getMockedResultSet());
    // validate rows
    Assert.assertEquals(readFinalOutputFile(new Path(formatter.getFinalOutputPath()), conf, "UTF-16LE"),
      getExpectedCSVRows());
  }

  /**
   * Test compression and encoding.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Test
  public void testCompressionAndEncoding() throws IOException {
    Configuration conf = new Configuration();
    conf.set(LensConfConstants.QUERY_OUTPUT_CHARSET_ENCODING, "UTF-16LE");
    conf.setBoolean(LensConfConstants.QUERY_OUTPUT_ENABLE_COMPRESSION, true);
    setConf(conf);
    testFormatter(conf, "UnicodeLittleUnmarked", LensConfConstants.RESULT_SET_PARENT_DIR_DEFAULT, ".csv.gz",
      getMockedResultSet());
    // validate rows
    Assert.assertEquals(readCompressedFile(new Path(formatter.getFinalOutputPath()), conf, "UTF-16LE"),
      getExpectedCSVRows());
  }

  /**
   * Test output path.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Test
  public void testOutputPath() throws IOException {
    Configuration conf = new Configuration();
    String outputParent = "target/" + getClass().getSimpleName();
    conf.set(LensConfConstants.RESULT_SET_PARENT_DIR, outputParent);
    setConf(conf);
    testFormatter(conf, "UTF8", outputParent, ".csv", getMockedResultSet());
    // validate rows
    Assert.assertEquals(readFinalOutputFile(new Path(formatter.getFinalOutputPath()), conf, "UTF-8"),
      getExpectedCSVRows());
  }

  /**
   * Test compression with custom output path.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Test
  public void testCompressionWithCustomOutputPath() throws IOException {
    Configuration conf = new Configuration();
    String outputParent = "target/" + getClass().getSimpleName();
    conf.set(LensConfConstants.RESULT_SET_PARENT_DIR, outputParent);
    conf.setBoolean(LensConfConstants.QUERY_OUTPUT_ENABLE_COMPRESSION, true);
    setConf(conf);
    testFormatter(conf, "UTF8", outputParent, ".csv.gz", getMockedResultSet());
    // validate rows
    Assert.assertEquals(readCompressedFile(new Path(formatter.getFinalOutputPath()), conf, "UTF-8"),
      getExpectedCSVRows());
  }

  /**
   * Creates the formatter.
   *
   * @return the wrapped file formatter
   */
  protected abstract WrappedFileFormatter createFormatter();

  /**
   * Write all rows.
   *
   * @param conf the conf
   * @throws IOException Signals that an I/O exception has occurred.
   */
  protected abstract void writeAllRows(Configuration conf) throws IOException;

  protected void setConf(Configuration conf) {
  }

  /**
   * Test formatter.
   *
   * @param conf            the conf
   * @param charsetEncoding the charset encoding
   * @param outputParentDir the output parent dir
   * @param fileExtn        the file extn
   * @param columnNames     the column names
   * @throws IOException Signals that an I/O exception has occurred.
   */
  protected void testFormatter(Configuration conf, String charsetEncoding, String outputParentDir, String fileExtn,
    LensResultSetMetadata columnNames) throws IOException {

    final LensDriver mockDriver = new MockDriver();
    try {
      mockDriver.configure(conf);
    } catch (LensException e) {
      Assert.fail(e.getMessage());
    }
    QueryContext ctx = new QueryContext("test writer query", "testuser", conf, new ArrayList<LensDriver>() {
      {
        add(mockDriver);
      }
    });

    ctx.setSelectedDriver(mockDriver);
    formatter = createFormatter();

    formatter.init(ctx, columnNames);

    // check output spec
    Assert.assertEquals(formatter.getEncoding(), charsetEncoding);
    Path tmpPath = formatter.getTmpPath();
    Path expectedTmpPath = new Path(outputParentDir, ctx.getQueryHandle() + ".tmp" + fileExtn);
    Assert.assertEquals(tmpPath, expectedTmpPath);

    // write header, rows and footer;
    formatter.writeHeader();
    writeAllRows(conf);
    formatter.writeFooter();
    FileSystem fs = expectedTmpPath.getFileSystem(conf);
    Assert.assertTrue(fs.exists(tmpPath));

    // commit and close
    formatter.commit();
    formatter.close();
    Assert.assertFalse(fs.exists(tmpPath));
    Path finalPath = new Path(formatter.getFinalOutputPath());
    Path expectedFinalPath = new Path(outputParentDir, ctx.getQueryHandle() + fileExtn).makeQualified(fs);
    Assert.assertEquals(finalPath, expectedFinalPath);
    Assert.assertTrue(fs.exists(finalPath));
  }

  /**
   * Read final output file.
   *
   * @param finalPath the final path
   * @param conf      the conf
   * @param encoding  the encoding
   * @return the list
   * @throws IOException Signals that an I/O exception has occurred.
   */
  protected List<String> readFinalOutputFile(Path finalPath, Configuration conf, String encoding) throws IOException {
    FileSystem fs = finalPath.getFileSystem(conf);
    return readFromStream(new InputStreamReader(fs.open(finalPath), encoding));
  }

  /**
   * Read compressed file.
   *
   * @param finalPath the final path
   * @param conf      the conf
   * @param encoding  the encoding
   * @return the list
   * @throws IOException Signals that an I/O exception has occurred.
   */
  protected List<String> readCompressedFile(Path finalPath, Configuration conf, String encoding) throws IOException {
    CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
    compressionCodecs = new CompressionCodecFactory(conf);
    final CompressionCodec codec = compressionCodecs.getCodec(finalPath);
    FileSystem fs = finalPath.getFileSystem(conf);
    return readFromStream(new InputStreamReader(codec.createInputStream(fs.open(finalPath)), encoding));
  }

  /**
   * Read from stream.
   *
   * @param ir the ir
   * @return the list
   * @throws IOException Signals that an I/O exception has occurred.
   */
  protected List<String> readFromStream(InputStreamReader ir) throws IOException {
    List<String> result = new ArrayList<String>();
    BufferedReader reader = new BufferedReader(ir);
    String line = reader.readLine();
    while (line != null) {
      result.add(line);
      line = reader.readLine();
    }
    reader.close();
    return result;

  }

  protected LensResultSetMetadata getMockedResultSet() {
    return new LensResultSetMetadata() {

      @Override
      public List<ColumnDescriptor> getColumns() {
        List<ColumnDescriptor> columns = new ArrayList<ColumnDescriptor>();
        columns.add(new ColumnDescriptor(new FieldSchema("firstcol", "int", ""), 0));
        columns.add(new ColumnDescriptor(new FieldSchema("format(secondcol,2)", "string", ""), 1));
        columns.add(new ColumnDescriptor(new FieldSchema("thirdcol", "varchar(20)", ""), 2));
        columns.add(new ColumnDescriptor(new FieldSchema("fourthcol", "char(15)", ""), 3));
        columns.add(new ColumnDescriptor(new FieldSchema("fifthcol", "array<tinyint>", ""), 4));
        columns.add(new ColumnDescriptor(new FieldSchema("sixthcol", "struct<a:int,b:varchar(10)>", ""), 5));
        columns.add(new ColumnDescriptor(new FieldSchema("seventhcol", "map<int,char(10)>", ""), 6));
        return columns;
      }
    };
  }

  protected LensResultSetMetadata getMockedResultSetWithoutComma() {
    return new LensResultSetMetadata() {

      @Override
      public List<ColumnDescriptor> getColumns() {
        List<ColumnDescriptor> columns = new ArrayList<ColumnDescriptor>();
        columns.add(new ColumnDescriptor(new FieldSchema("firstcol", "int", ""), 0));
        columns.add(new ColumnDescriptor(new FieldSchema("secondcol", "string", ""), 1));
        columns.add(new ColumnDescriptor(new FieldSchema("thirdcol", "varchar(20)", ""), 2));
        columns.add(new ColumnDescriptor(new FieldSchema("fourthcol", "char(15)", ""), 3));
        columns.add(new ColumnDescriptor(new FieldSchema("fifthcol", "array<tinyint>", ""), 4));
        columns.add(new ColumnDescriptor(new FieldSchema("sixthcol", "struct<a:int,b:varchar(10)>", ""), 5));
        columns.add(new ColumnDescriptor(new FieldSchema("seventhcol", "map<int,char(10)>", ""), 6));
        return columns;
      }
    };
  }

  /**
   * Read zip output file.
   *
   * @param finalPath the final path
   * @param conf      the conf
   * @param encoding  the encoding
   * @return the list
   * @throws IOException Signals that an I/O exception has occurred.
   */
  protected List<String> readZipOutputFile(Path finalPath, Configuration conf, String encoding) throws IOException {
    FileSystem fs = finalPath.getFileSystem(conf);
    List<String> result = new ArrayList<String>();
    ZipEntry ze = null;
    ZipInputStream zin = new ZipInputStream(fs.open(finalPath));
    while ((ze = zin.getNextEntry()) != null) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(zin, encoding));
      String line = reader.readLine();
      while (line != null) {
        result.add(line);
        line = reader.readLine();
      }
      zin.closeEntry();
    }
    zin.close();
    return result;
  }

  protected abstract List<String> getExpectedCSVRows();

  protected abstract List<String> getExpectedTextRows();

  protected abstract List<String> getExpectedCSVRowsWithoutComma();

  protected abstract List<String> getExpectedTextRowsWithoutComma();

  protected abstract List<String> getExpectedCSVRowsWithMultiple();

  protected abstract List<String> getExpectedTextRowsWithMultiple();

  protected abstract List<String> getExpectedCSVRowsWithMultipleWithoutComma();

  protected abstract List<String> getExpectedTextRowsWithMultipleWithoutComma();
}
