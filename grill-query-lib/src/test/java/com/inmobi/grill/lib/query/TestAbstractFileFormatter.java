package com.inmobi.grill.lib.query;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.inmobi.grill.api.query.ResultColumn;
import com.inmobi.grill.lib.query.GrillFileOutputFormat.GrillRowWriter;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.driver.GrillResultSetMetadata;
import com.inmobi.grill.server.api.query.QueryContext;

public abstract class TestAbstractFileFormatter {

  protected FileFormatter formatter;

  @AfterMethod
  public void cleanup() throws IOException {
    if (formatter != null) {
      FileSystem fs = new Path(formatter.getFinalOutputPath()).getFileSystem(new Configuration());
      fs.delete(new Path(formatter.getFinalOutputPath()), true);
    }
  }

  @Test
  public void testFormatter() throws IOException {
    Configuration conf = new Configuration();
    setConf(conf);
    testFormatter(conf, "UTF8",
        GrillConfConstants.GRILL_RESULT_SET_PARENT_DIR_DEFAULT, ".csv");
    // validate rows
    Assert.assertEquals(readFinalOutputFile(
        new Path(formatter.getFinalOutputPath()), conf, "UTF-8"), getExpectedCSVRows());
  }

  @Test
  public void testCompression() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(GrillConfConstants.QUERY_OUTPUT_ENABLE_COMPRESSION, true);
    setConf(conf);
    testFormatter(conf, "UTF8",
        GrillConfConstants.GRILL_RESULT_SET_PARENT_DIR_DEFAULT, ".csv.gz");
    // validate rows
    Assert.assertEquals(readCompressedFile(
        new Path(formatter.getFinalOutputPath()), conf, "UTF-8"), getExpectedCSVRows());
  }

  @Test
  public void testCustomCompression() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(GrillConfConstants.QUERY_OUTPUT_ENABLE_COMPRESSION, true);
    conf.set(GrillConfConstants.QUERY_OUTPUT_COMPRESSION_CODEC,
        org.apache.hadoop.io.compress.DefaultCodec.class.getCanonicalName());
    setConf(conf);
    testFormatter(conf, "UTF8",
        GrillConfConstants.GRILL_RESULT_SET_PARENT_DIR_DEFAULT, ".csv.deflate");
    // validate rows
    Assert.assertEquals(readCompressedFile(
        new Path(formatter.getFinalOutputPath()), conf, "UTF-8"), getExpectedCSVRows());
  }

  @Test
  public void testEncoding() throws IOException {
    Configuration conf = new Configuration();
    conf.set(GrillConfConstants.QUERY_OUTPUT_CHARSET_ENCODING, "UTF-16LE");
    setConf(conf);
    testFormatter(conf, "UnicodeLittleUnmarked",
        GrillConfConstants.GRILL_RESULT_SET_PARENT_DIR_DEFAULT, ".csv");
    // validate rows
    Assert.assertEquals(readFinalOutputFile(
        new Path(formatter.getFinalOutputPath()), conf, "UTF-16LE"), getExpectedCSVRows());
  }

  @Test
  public void testCompressionAndEncoding() throws IOException {
    Configuration conf = new Configuration();
    conf.set(GrillConfConstants.QUERY_OUTPUT_CHARSET_ENCODING, "UTF-16LE");
    conf.setBoolean(GrillConfConstants.QUERY_OUTPUT_ENABLE_COMPRESSION, true);
    setConf(conf);
    testFormatter(conf, "UnicodeLittleUnmarked",
        GrillConfConstants.GRILL_RESULT_SET_PARENT_DIR_DEFAULT, ".csv.gz");
    // validate rows
    Assert.assertEquals(readCompressedFile(
        new Path(formatter.getFinalOutputPath()), conf, "UTF-16LE"), getExpectedCSVRows());
  }

  @Test
  public void testOutputPath() throws IOException {
    Configuration conf = new Configuration();
    String outputParent = "target/" + getClass().getSimpleName();
    conf.set(GrillConfConstants.GRILL_RESULT_SET_PARENT_DIR, outputParent);
    setConf(conf);
    testFormatter(conf, "UTF8", outputParent, ".csv");
    // validate rows
    Assert.assertEquals(readFinalOutputFile(
        new Path(formatter.getFinalOutputPath()), conf, "UTF-8"), getExpectedCSVRows());
  }

  @Test
  public void testCompressionWithCustomOutputPath() throws IOException {
    Configuration conf = new Configuration();
    String outputParent = "target/" + getClass().getSimpleName();
    conf.set(GrillConfConstants.GRILL_RESULT_SET_PARENT_DIR, outputParent);
    conf.setBoolean(GrillConfConstants.QUERY_OUTPUT_ENABLE_COMPRESSION, true);
    setConf(conf);
    testFormatter(conf, "UTF8", outputParent, ".csv.gz");
    // validate rows
    Assert.assertEquals(readCompressedFile(
        new Path(formatter.getFinalOutputPath()), conf, "UTF-8"), getExpectedCSVRows());
  }

  protected abstract FileFormatter createFormatter();

  protected abstract void writeAllRows(Configuration conf) throws IOException;

  protected void setConf(Configuration conf) {
  }

  protected void testFormatter(Configuration conf, String charsetEncoding,
      String outputParentDir,
      String fileExtn) throws IOException {
    QueryContext ctx = new QueryContext("test writer query", "testuser", null, conf);
    formatter = createFormatter();

    formatter.setConf(conf);
    formatter.init(ctx, getMockedResultSet());

    // check output spec
    GrillRowWriter rowWriter = formatter.getRowWriter();
    Assert.assertEquals(rowWriter.getEncoding(), charsetEncoding);
    Path tmpPath = rowWriter.getTmpPath();
    Path expectedTmpPath = new Path (outputParentDir,
        ctx.getQueryHandle() + ".tmp" + fileExtn);
    Assert.assertEquals(tmpPath, expectedTmpPath);

    // write header, rows and footer; 
    formatter.writeHeader();
    writeAllRows(conf);
    formatter.writeFooter();
    FileSystem fs = expectedTmpPath.getFileSystem(conf);
    Assert.assertTrue(fs.exists(tmpPath));

    //commit and close
    formatter.commit();
    formatter.close();
    Assert.assertFalse(fs.exists(tmpPath));
    Path finalPath = new Path(formatter.getFinalOutputPath());
    Path expectedFinalPath = new Path (outputParentDir,
        ctx.getQueryHandle() + fileExtn).makeQualified(fs);
    Assert.assertEquals(finalPath, expectedFinalPath);
    Assert.assertTrue(fs.exists(finalPath));
  }

  protected List<String> readFinalOutputFile(Path finalPath, Configuration conf,
      String encoding) throws IOException {
    FileSystem fs = finalPath.getFileSystem(conf);
    return readFromStream(new InputStreamReader(fs.open(finalPath), encoding));
  }

  protected List<String> readCompressedFile(Path finalPath, Configuration conf,
      String encoding) throws IOException {
    CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
    compressionCodecs = new CompressionCodecFactory(conf);
    final CompressionCodec codec = compressionCodecs.getCodec(finalPath);
    FileSystem fs = finalPath.getFileSystem(conf);
    return readFromStream(new InputStreamReader(
        codec.createInputStream(fs.open(finalPath)), encoding));
  }

  private List<String> readFromStream(InputStreamReader ir) throws IOException {
    List<String> result = new ArrayList<String>();
    BufferedReader reader = new BufferedReader(ir);
    String line = reader.readLine();
    while (line != null) {
      result.add(line);
      line = reader.readLine();
    }
    return result;

  }
  private GrillResultSetMetadata getMockedResultSet() {
    return new GrillResultSetMetadata() {

      @Override
      public List<ResultColumn> getColumns() {
        List<ResultColumn> columns = new ArrayList<ResultColumn>();
        columns.add(new ResultColumn("firstcol", "int"));
        columns.add(new ResultColumn("secondcol", "string"));
        return columns;
      }
    };
  }

  protected List<String> getExpectedCSVRows() {
    List<String> csvRows = new ArrayList<String>();
    csvRows.add("\"firstcol\",\"secondcol\"");
    csvRows.add("\"1\",\"one\"");
    csvRows.add("\"2\",\"two\"");
    csvRows.add("\"NULL\",\"three\"");
    csvRows.add("\"4\",\"NULL\"");
    csvRows.add("\"NULL\",\"NULL\"");
    csvRows.add("Total rows:5");
    return csvRows;
  }

  protected List<String> getExpectedTextRows() {
    List<String> txtRows = new ArrayList<String>();
    txtRows.add("firstcolsecondcol");
    txtRows.add("1one");
    txtRows.add("2two");
    txtRows.add("\\Nthree");
    txtRows.add("4\\N");
    txtRows.add("\\N\\N");
    txtRows.add("Total rows:5");
    return txtRows;
  }

}
