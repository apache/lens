package com.inmobi.grill.ml;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.testng.annotations.Test;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class TestExampleUtils {

  @Test
  public void testCreateExampleTable() throws Exception {
    File f = new File("target/testdata");
    if (!f.exists()) {
      f.mkdir();
    }


    String dataFile = "target/testdata/test.data";
    PrintWriter writer = new PrintWriter(dataFile);
    for (int i = 0; i < 100; i++) {
      writer.println("1 2 3 4");
    }

    writer.flush();
    writer.close();
    HiveConf conf = new HiveConf();
    ExampleUtils.createExampleTable(conf, "default", "test_table", dataFile,
      "label", "f1", "f2", "f3");

    Table tbl = Hive.get(conf).getTable("default", "test_table");
    assertNotNull(tbl);

    List<FieldSchema> columns = tbl.getCols();

    assertEquals(columns.get(0).getName(), "label");
    assertEquals(columns.get(1).getName(), "f1");
    assertEquals(columns.get(2).getName(), "f2");
    assertEquals(columns.get(3).getName(), "f3");

    List<Partition> parts = Hive.get(conf).getPartitions(tbl);
    assertEquals(parts.size(), 1);
    Partition part = parts.get(0);
    assertEquals(part.getLocation() + "/",
      new File(dataFile).getParentFile().toURI().toString());
    Hive.get(conf).dropTable("default", "test_table");
  }

}
