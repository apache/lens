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
package org.apache.lens.cube.parse;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lens.cube.metadata.StorageConstants;

class StorageUtil {
  private static final Log LOG = LogFactory.getLog(StorageUtil.class.getName());

  public static String getWherePartClause(String timeDimName, String tableName, List<String> parts) {
    if (parts.size() == 0) {
      return "";
    }
    StringBuilder partStr = new StringBuilder();
    String sep = "";
    for (int i = 0; i < parts.size(); i++) {
      partStr.append(sep);
      partStr.append("(");
      partStr.append(tableName);
      partStr.append(".");
      partStr.append(timeDimName);
      partStr.append(" = '");
      partStr.append(parts.get(i));
      partStr.append("'");
      partStr.append(")");
      sep = " OR ";
    }
    return partStr.toString();
  }

  public static String getNotLatestClauseForDimensions(String cubeName, Set<String> timedDimensions) {
    StringBuilder sb = new StringBuilder();
    String sep = "";
    for(String timePartCol: timedDimensions) {
        sb.append(sep).append(cubeName).append(".").append(timePartCol).
          append("!=").append(StorageConstants.LATEST_PARTITION_VALUE);
        sep = " AND ";
    }
    return sb.toString();
  }

  public static String joinWithAnd(String clause1, String clause2) {
    return new StringBuilder()
      .append("((")
      .append(clause1)
      .append(") AND (")
      .append(clause2)
      .append("))")
      .toString();
  }

  /**
   * Get minimal set of storages which cover the queried partitions
   * 
   * @param answeringParts
   *          Map from partition to set of answering storage tables
   * @param minimalStorageTables
   *          from storage to covering parts
   * 
   * @return true if multi table select is enabled, false otherwise
   */
  static boolean getMinimalAnsweringTables(List<FactPartition> answeringParts,
      Map<String, Set<FactPartition>> minimalStorageTables) {
    // map from storage table to the partitions it covers
    Map<String, Set<FactPartition>> invertedMap = new HashMap<String, Set<FactPartition>>();
    boolean enableMultiTableSelect = true;
    // invert the answering tables map and put in inverted map
    for (FactPartition part : answeringParts) {
      for (String table : part.getStorageTables()) {
        Set<FactPartition> partsCovered = invertedMap.get(table);
        if (partsCovered == null) {
          partsCovered = new TreeSet<FactPartition>();
          invertedMap.put(table, partsCovered);
        }
        partsCovered.add(part);
      }
    }
    // there exist only one storage
    if (invertedMap.size() != 1) {
      Set<FactPartition> remaining = new TreeSet<FactPartition>();
      remaining.addAll(answeringParts);
      while (!remaining.isEmpty()) {
        // returns a singleton map
        Map<String, Set<FactPartition>> maxCoveringStorage = getMaxCoveringStorage(invertedMap, remaining);
        minimalStorageTables.putAll(maxCoveringStorage);
        Set<FactPartition> coveringSet = maxCoveringStorage.values().iterator().next();
        if (enableMultiTableSelect) {
          if (!coveringSet.containsAll(invertedMap.get(maxCoveringStorage.keySet().iterator().next()))) {
            LOG.info("Disabling multi table select" + " because the partitions are not mutually exclusive");
            enableMultiTableSelect = false;
          }
        }
        remaining.removeAll(coveringSet);
      }
    } else {
      minimalStorageTables.putAll(invertedMap);
    }
    return enableMultiTableSelect;
  }

  private static Map<String, Set<FactPartition>> getMaxCoveringStorage(
      final Map<String, Set<FactPartition>> storageCoveringMap, Set<FactPartition> queriedParts) {
    int coveringcount = 0;
    int maxCoveringCount = 0;
    String maxCoveringStorage = null;
    Set<FactPartition> maxCoveringSet = null;
    for (Map.Entry<String, Set<FactPartition>> entry : storageCoveringMap.entrySet()) {
      Set<FactPartition> coveringSet = new TreeSet<FactPartition>();
      coveringSet.addAll(entry.getValue());
      coveringSet.retainAll(queriedParts);
      coveringcount = coveringSet.size();
      if (coveringcount > maxCoveringCount) {
        maxCoveringCount = coveringcount;
        maxCoveringStorage = entry.getKey();
        maxCoveringSet = coveringSet;
      }
    }
    return Collections.singletonMap(maxCoveringStorage, maxCoveringSet);
  }
}
