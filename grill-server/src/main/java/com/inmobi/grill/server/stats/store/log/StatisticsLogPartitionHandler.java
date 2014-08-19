package com.inmobi.grill.server.stats.store.log;
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
import com.inmobi.grill.server.api.events.AsyncEventListener;
import com.inmobi.grill.server.stats.event.LoggableGrillStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Class used to copy log files to HDFS and add partition to hive metastore.
 */
public class StatisticsLogPartitionHandler extends AsyncEventListener<PartitionEvent> {

  private static final Logger LOG =
      LoggerFactory.getLogger(StatisticsLogPartitionHandler.class);
  private Path warehousePath;
  private Hive client;
  private String database;


  public void initialize(Configuration conf) {
    String temp = conf.get(GrillConfConstants.GRILL_STATISTICS_WAREHOUSE_KEY,
        GrillConfConstants.DEFAULT_STATISTICS_WAREHOUSE);
    warehousePath = new Path(temp);
    database = conf.get(GrillConfConstants.GRILL_STATISTICS_DATABASE_KEY,
        GrillConfConstants.DEFAULT_STATISTICS_DATABASE);
    try {
      client = Hive.get();
    } catch (Exception e) {
      LOG.error("Unable to connect to hive metastore", e);
      throw new IllegalArgumentException("Unable to connect to hive metastore", e);
    }
  }

  @Override
  public void process(PartitionEvent event) {
    String eventName = event.getEventName();
    Map<String, String> partitionMap = event.getPartMap();
    Path eventDir = new Path(warehousePath, eventName);
    for (Map.Entry<String, String> entry : partitionMap.entrySet()) {
      String partitionPath = entry.getKey().replace("-", "/");
      Path finalPath = new Path(eventDir, partitionPath + "/" + eventName + ".log");
      try {
        copyToHdfs(entry.getValue(), finalPath);
        boolean added = addPartition(eventName, entry.getKey(), finalPath, event.getClassName());
        if (added) {
          new File(entry.getValue()).delete();
        }
      } catch (Exception e) {
        LOG.error("Unable to copy file to the file system", e);
      }
    }
  }

  private boolean addPartition(String eventName, String key, Path finalPath,
                               String className) {

    try {
      Table t = getTable(eventName, className);
      HashMap<String, String> partSpec = new HashMap<String, String>();
      partSpec.put("dt", key);
      Partition p = client.createPartition(t, partSpec);
      p.setLocation(finalPath.toString());
      client.alterPartition(database, eventName, p);
      return true;
    } catch (Exception e) {
      LOG.warn("Unable to add the partition ", e);
      return false;
    }
  }

  private Table getTable(String eventName, String className) throws Exception {
    Table tmp = null;
    try {
      tmp = client.getTable(database, eventName, false);
      if(tmp == null) {
        tmp = createTable(eventName, className);
      }
    } catch (HiveException e) {
      LOG.warn("Exception thrown while creating the table", e);
    }
    return tmp;
  }

  private Table createTable(String eventName, String className) throws Exception {
    Table tmp;
    try {
      Database db = new Database();
      db.setName(database);
      client.createDatabase(db, true);
      Class<LoggableGrillStatistics> statisticsClass = (Class<LoggableGrillStatistics>)
          Class.forName(className);
      LoggableGrillStatistics stat = statisticsClass.newInstance();
      Configuration conf = new Configuration();
      conf.addResource("hive-site.xml");
      tmp = stat.getHiveTable(conf);
      tmp.setDbName(database);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating table  " + tmp.getTableName());
      }
      client.createTable(tmp);
      tmp = client.getTable(database, eventName);
    } catch (Exception e1) {
      LOG.warn("Unable to create hive table, exiting", e1);
      throw e1;
    }
    return tmp;
  }

  private void copyToHdfs(String localPath, Path finalPath) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = finalPath.getFileSystem(conf);
    if (fs.exists(finalPath)) {
      fs.delete(finalPath, true);
    }
    IOUtils.copyBytes(new FileInputStream(localPath),
        fs.create(finalPath), conf, true);
  }
}
