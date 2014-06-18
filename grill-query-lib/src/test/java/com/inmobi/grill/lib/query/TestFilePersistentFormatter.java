package com.inmobi.grill.lib.query;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
    writer.write("\"1\",\"one\"\n");
    writer.close();
    writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(partFileDir, "000001_0"))));
    writer.write("\"2\",\"two\"\n");
    writer.write("\"NULL\",\"three\"\n");
    writer.close();
    writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(partFileDir, "000010_1"))));
    writer.write("\"4\",\"NULL\"\n");
    writer.write("\"NULL\",\"NULL\"\n");
    writer.close();
    writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(partFileDir, "_SUCCESS"))));
    writer.close();

    // create text files
    fs = partFileTextDir.getFileSystem(new Configuration());
    writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(partFileTextDir, "000000_2"))));
    writer.write("1one\n");
    writer.close();
    writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(partFileTextDir, "000001_0"))));
    writer.write("2two\n");
    writer.write("\\Nthree\n");
    writer.close();
    writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(partFileTextDir, "000010_1"))));
    writer.write("4\\N\n");
    writer.write("\\N\\N\n");
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
  protected FileFormatter createFormatter() {
    return new FilePersistentFormatter();
  }

  @Override
  protected void writeAllRows(Configuration conf) throws IOException {
    ((PersistedOutputFormatter)formatter).addRowsFromPersistedPath(new Path(conf.get("test.partfile.dir")));
  }

  protected void setConf(Configuration conf) {
    conf.set("test.partfile.dir", partFileDir.toString());
    conf.set(GrillConfConstants.QUERY_OUTPUT_HEADER, "\"firstcol\",\"secondcol\"");
    conf.set(GrillConfConstants.QUERY_OUTPUT_FOOTER, "Total rows:5");
  }

  @Test
  public void testTextFiles() throws IOException {
    Configuration conf = new Configuration();
    setConf(conf);
    conf.set("test.partfile.dir", partFileTextDir.toString());
    conf.set(GrillConfConstants.QUERY_OUTPUT_FILE_EXTN, ".txt");
    conf.set(GrillConfConstants.QUERY_OUTPUT_HEADER, "firstcolsecondcol");
    testFormatter(conf, "UTF8",
        GrillConfConstants.GRILL_RESULT_SET_PARENT_DIR_DEFAULT, ".txt");
    // validate rows
    Assert.assertEquals(readFinalOutputFile(
        new Path(formatter.getFinalOutputPath()), conf, "UTF-8"), getExpectedTextRows());

  }

  @Test
  public void testTextFilesWithCompression() throws IOException {
    Configuration conf = new Configuration();
    setConf(conf);
    conf.set("test.partfile.dir", partFileTextDir.toString());
    conf.set(GrillConfConstants.QUERY_OUTPUT_FILE_EXTN, ".txt");
    conf.setBoolean(GrillConfConstants.QUERY_OUTPUT_ENABLE_COMPRESSION, true);
    conf.set(GrillConfConstants.QUERY_OUTPUT_HEADER, "firstcolsecondcol");
    testFormatter(conf, "UTF8",
        GrillConfConstants.GRILL_RESULT_SET_PARENT_DIR_DEFAULT, ".txt.gz");
    // validate rows
    Assert.assertEquals(readCompressedFile(
        new Path(formatter.getFinalOutputPath()), conf, "UTF-8"), getExpectedTextRows());
  }

}
