package com.inmobi.grill.server.stats;
 /*
 * #%L
 * Grill Server
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


import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.stats.event.query.QueryExecutionStatistics;
import com.inmobi.grill.server.stats.store.log.PartitionEvent;
import com.inmobi.grill.server.stats.store.log.StatisticsLogPartitionHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;

import static org.testng.Assert.assertNotNull;

public class TestStatisticsLogPartitionHandler {


  public static final String EVENT_NAME = "DummyStats";

  @Test
  public void testPartitionHandler() throws Exception {
    Configuration conf = configureHiveTables();
    String fileName = "/tmp/grillstats.log";
    File f = createDummyFile(fileName);
    StatisticsLogPartitionHandler handler = new StatisticsLogPartitionHandler();
    handler.initialize(conf);
    HashMap<String, String> partMap = new HashMap<String, String>();
    partMap.put("random", f.getAbsolutePath());
    PartitionEvent event = new PartitionEvent(EVENT_NAME, partMap, null);
    handler.process(event);
    Hive h = getHiveClient();
    Set<Partition> partitionSet = h.getAllPartitionsOf(getHiveTable());
    Assert.assertEquals(partitionSet.size(), 1);
    Partition p = partitionSet.iterator().next();
    Assert.assertEquals(p.getTable().getTableName(),
        EVENT_NAME);
    Assert.assertEquals(p.getTable().getDbName(),
        GrillConfConstants.DEFAULT_STATISTICS_DATABASE);
    Assert.assertEquals(p.getDataLocation(),
        new Path(GrillConfConstants.
            DEFAULT_STATISTICS_WAREHOUSE, EVENT_NAME + "/random/" + EVENT_NAME + ".log"));
    Assert.assertFalse(f.exists());
    h.dropTable(GrillConfConstants.DEFAULT_STATISTICS_DATABASE,
        EVENT_NAME, true, true);
  }


  @Test
  public void testQueryExecutionStatisticsTableCreation() throws Exception {
    QueryExecutionStatistics stats =
        new QueryExecutionStatistics(System.currentTimeMillis());
    Configuration conf = new Configuration();
    conf.addResource("hive-site.xml");
    Table t = stats.getHiveTable(conf);
    Hive h = getHiveClient();
    h.createTable(t);
    Assert.assertNotNull(h.getTable(GrillConfConstants.DEFAULT_STATISTICS_DATABASE,
        t.getTableName()));
    h.dropTable(GrillConfConstants.DEFAULT_STATISTICS_DATABASE, t.getTableName(),
        true, true);
  }

  private File createDummyFile(String fileName) throws IOException {
    File f = new File(fileName);
    f.createNewFile();
    return f;
  }

  private Configuration configureHiveTables() {
    assertNotNull(System.getProperty("hadoop.bin.path"));
    Configuration conf = new Configuration();
    conf.addResource("hive-site.xml");
    try {
      Hive hive = getHiveClient();
      Database database = new Database();
      database.setName(GrillConfConstants.DEFAULT_STATISTICS_DATABASE);
      hive.dropTable(GrillConfConstants.DEFAULT_STATISTICS_DATABASE,
          EVENT_NAME, true, true);
      hive.dropTable(GrillConfConstants.DEFAULT_STATISTICS_DATABASE,
          QueryExecutionStatistics.class.getSimpleName(), true, true);
      hive.dropDatabase(GrillConfConstants.DEFAULT_STATISTICS_DATABASE, true, true);
      hive.createDatabase(database);
      Table t = getHiveTable();
      hive.createTable(t);
    } catch (Exception e) {
      Assert.fail();
    }
    return conf;
  }

  private Table getHiveTable() {
    Table t = new Table(GrillConfConstants.DEFAULT_STATISTICS_DATABASE, EVENT_NAME);
    LinkedList<FieldSchema> partCols = new LinkedList<FieldSchema>();
    partCols.add(new FieldSchema("dt", "string", "partCol"));
    t.setPartCols(partCols);
    return t;
  }

  private Hive getHiveClient() throws HiveException {
    return Hive.get();
  }

}
