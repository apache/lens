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

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.server.api.events.GrillEventService;
import lombok.Setter;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Timer class for monitoring log file rollup.
 */
public class StatisticsLogFileScannerTask extends TimerTask {

  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(StatisticsLogFileScannerTask.class);


  private Map<String, String> scanSet = new ConcurrentHashMap<String, String>();

  @Setter
  private GrillEventService service;

  private Map<String, String> classSet = new ConcurrentHashMap<String, String>();

  @Override
  public void run() {
    for (Map.Entry<String, String> entry : scanSet.entrySet()) {
      File f = new File(entry.getValue()).getAbsoluteFile();
      String fileName = f.getAbsolutePath();
      File[] latestLogFiles = getLatestLogFile(fileName);
      HashMap<String, String> partMap = getPartMap(fileName,
          latestLogFiles);
      String eventName = entry.getKey();
      PartitionEvent event = new PartitionEvent(eventName, partMap,
          classSet.get(eventName));
      try {
        service.notifyEvent(event);
      } catch (GrillException e) {
        LOG.warn("Unable to Notify partition event" +
            event.getEventName() + " with map  " + event.getPartMap());
      }
    }

  }

  private HashMap<String, String> getPartMap(String fileName, File[] latestLogFiles) {
    HashMap<String, String> partMap = new HashMap<String, String>();
    for (File f : latestLogFiles) {
      partMap.put(f.getPath().replace(fileName + '.', ""),
          f.getAbsolutePath());
    }
    return partMap;
  }

  private File[] getLatestLogFile(String value) {
    File f = new File(value);
    final String fileNamePattern = f.getName();
    File parent = f.getParentFile();
    return parent.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File file, String name) {
        return !name.equals(fileNamePattern)
            && name.contains(fileNamePattern);
      }
    });
  }

  public void addLogFile(String event) {
    if (scanSet.containsKey(event)) {
      return;
    }
    String appenderName = event.substring(event.lastIndexOf(".") + 1,
        event.length());
    Logger log = Logger.getLogger(event);
    if (log.getAppender(appenderName) == null) {
      LOG.error("Unable to find " +
          "statistics log appender for  " + event
          + " with appender name " + appenderName);
      return;
    }
    String location = ((FileAppender)
        log.getAppender(appenderName)).getFile();
    scanSet.put(appenderName, location);
    classSet.put(appenderName, event);
  }


}
