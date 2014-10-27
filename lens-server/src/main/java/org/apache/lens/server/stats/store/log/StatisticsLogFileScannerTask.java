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
package org.apache.lens.server.stats.store.log;

import lombok.Setter;

import org.apache.lens.api.LensException;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.events.LensEventService;
import org.apache.lens.server.api.metrics.MetricsService;
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

  /** The Constant LOG. */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(StatisticsLogFileScannerTask.class);

  /** The Constant LOG_SCANNER_ERRORS. */
  public static final String LOG_SCANNER_ERRORS = "log-scanner-errors";

  /** The scan set. */
  private Map<String, String> scanSet = new ConcurrentHashMap<String, String>();

  /** The service. */
  @Setter
  private LensEventService service;

  /** The class set. */
  private Map<String, String> classSet = new ConcurrentHashMap<String, String>();

  /*
   * (non-Javadoc)
   * 
   * @see java.util.TimerTask#run()
   */
  @Override
  public void run() {
    try {
      for (Map.Entry<String, String> entry : scanSet.entrySet()) {
        File f = new File(entry.getValue()).getAbsoluteFile();
        String fileName = f.getAbsolutePath();
        File[] latestLogFiles = getLatestLogFile(fileName);
        HashMap<String, String> partMap = getPartMap(fileName, latestLogFiles);
        String eventName = entry.getKey();
        PartitionEvent event = new PartitionEvent(eventName, partMap, classSet.get(eventName));
        try {
          service.notifyEvent(event);
        } catch (LensException e) {
          LOG.warn("Unable to Notify partition event" + event.getEventName() + " with map  " + event.getPartMap());
        }
      }
    } catch (Exception exc) {
      MetricsService svc = (MetricsService) LensServices.get().getService(MetricsService.NAME);
      svc.incrCounter(StatisticsLogFileScannerTask.class, LOG_SCANNER_ERRORS);
      LOG.error("Unknown error in log file scanner ", exc);
    }
  }

  /**
   * Gets the part map.
   *
   * @param fileName
   *          the file name
   * @param latestLogFiles
   *          the latest log files
   * @return the part map
   */
  private HashMap<String, String> getPartMap(String fileName, File[] latestLogFiles) {
    HashMap<String, String> partMap = new HashMap<String, String>();
    for (File f : latestLogFiles) {
      partMap.put(f.getPath().replace(fileName + '.', ""), f.getAbsolutePath());
    }
    return partMap;
  }

  /**
   * Gets the latest log file.
   *
   * @param value
   *          the value
   * @return the latest log file
   */
  private File[] getLatestLogFile(String value) {
    File f = new File(value);
    final String fileNamePattern = f.getName();
    File parent = f.getParentFile();
    return parent.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File file, String name) {
        return !new File(name).isHidden() && !name.equals(fileNamePattern) && name.contains(fileNamePattern);
      }
    });
  }

  /**
   * Adds the log file.
   *
   * @param event
   *          the event
   */
  public void addLogFile(String event) {
    if (scanSet.containsKey(event)) {
      return;
    }
    String appenderName = event.substring(event.lastIndexOf(".") + 1, event.length());
    Logger log = Logger.getLogger(event);
    if (log.getAppender(appenderName) == null) {
      LOG.error("Unable to find " + "statistics log appender for  " + event + " with appender name " + appenderName);
      return;
    }
    String location = ((FileAppender) log.getAppender(appenderName)).getFile();
    scanSet.put(appenderName, location);
    classSet.put(appenderName, event);
  }

}
