/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.cube.parse;

import static org.apache.lens.cube.metadata.DateUtil.WSPACE;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lens.cube.metadata.*;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Lists;

public final class StorageUtil {
  private StorageUtil() {

  }

  public static String getWherePartClause(String timeDimName, String tableName, Collection<String> parts) {
    if (parts.size() == 0) {
      return "";
    }
    StringBuilder partStr = new StringBuilder();
    String sep = "";
    for (String part : parts) {
      partStr.append(sep);
      partStr.append("(");
      partStr.append(tableName != null ? tableName : "%s");
      partStr.append(".");
      partStr.append(timeDimName);
      partStr.append(" = '");
      partStr.append(part);
      partStr.append("'");
      partStr.append(")");
      sep = " OR ";
    }
    return partStr.toString();
  }

  public static String getWherePartClauseWithIn(String timeDimName, String tableName, List<String> parts) {
    if (parts.size() == 0) {
      return "";
    }
    StringBuilder inClause = new StringBuilder();
    String sep = "";
    for (String part : parts) {
      inClause.append(sep).append("'").append(part).append("'");
      sep = ",";
    }
    return tableName + "." + timeDimName + " IN (" + inClause + ")";
  }

  public static String getNotLatestClauseForDimensions(String alias, Set<String> timedDimensions, String partCol) {
    StringBuilder sb = new StringBuilder();
    String sep = "";
    for (String timePartCol : timedDimensions) {
      if (!timePartCol.equals(partCol)) {
        sb.append(sep).append(alias).append(".").append(timePartCol).append(" != '")
          .append(StorageConstants.LATEST_PARTITION_VALUE).append("'");
        sep = " AND ";
      }
    }
    return sb.toString();
  }

  /**
   * Get minimal set of storages which cover the queried partitions
   *
   * @param answeringParts       Map from partition to set of answering storage tables
   * @param minimalStorageTables from storage to covering parts
   */
  static void getMinimalAnsweringTables(List<FactPartition> answeringParts,
    Map<String, Set<FactPartition>> minimalStorageTables) {
    // map from storage table to the partitions it covers
    Map<String, Set<FactPartition>> invertedMap = new HashMap<String, Set<FactPartition>>();
    // invert the answering tables map and put in inverted map
    for (FactPartition part : answeringParts) {
      for (String table : part.getStorageTables()) {
        invertedMap.computeIfAbsent(table, k -> new TreeSet<>()).add(part);
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
        remaining.removeAll(coveringSet);
      }
    } else {
      minimalStorageTables.putAll(invertedMap);
    }
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

  public static String getWhereClause(String clause, String alias) {
    return String.format(clause, alias);
  }

  public static String getWhereClause(CandidateDim dim, String alias) {
    if (!dim.isWhereClauseAdded(alias) && !StringUtils.isBlank(dim.getWhereClause())) {
      return getWhereClause(dim.getWhereClause(), alias);
    } else {
      return null;
    }
  }

  /**
   * Get fallback range
   *
   * @param range
   * @param factName
   * @param cubeql
   * @return
   * @throws LensException
   */
  public static TimeRange getFallbackRange(TimeRange range, String factName, CubeQueryContext cubeql)
    throws LensException {
    Cube baseCube = cubeql.getBaseCube();
    ArrayList<String> tableNames = Lists.newArrayList(factName, cubeql.getCube().getName());
    if (!cubeql.getCube().getName().equals(baseCube.getName())) {
      tableNames.add(baseCube.getName());
    }
    String fallBackString = null;
    String timedim = baseCube.getTimeDimOfPartitionColumn(range.getPartitionColumn());
    for (String tableName : tableNames) {
      fallBackString = cubeql.getMetastoreClient().getTable(tableName).getParameters()
        .get(MetastoreConstants.TIMEDIM_RELATION + timedim);
      if (StringUtils.isNotBlank(fallBackString)) {
        break;
      }
    }
    if (StringUtils.isBlank(fallBackString)) {
      return null;
    }
    Matcher matcher = Pattern.compile("(.*?)\\+\\[(.*?),(.*?)\\]").matcher(fallBackString.replaceAll(WSPACE, ""));
    if (!matcher.matches()) {
      return null;
    }
    DateUtil.TimeDiff diff1 = DateUtil.TimeDiff.parseFrom(matcher.group(2).trim());
    DateUtil.TimeDiff diff2 = DateUtil.TimeDiff.parseFrom(matcher.group(3).trim());
    String relatedTimeDim = matcher.group(1).trim();
    String fallbackPartCol = baseCube.getPartitionColumnOfTimeDim(relatedTimeDim);
    return TimeRange.builder().fromDate(diff2.negativeOffsetFrom(range.getFromDate()))
      .toDate(diff1.negativeOffsetFrom(range.getToDate())).partitionColumn(fallbackPartCol).build();
  }

  /**
   * Checks how much data is completed for a column.
   * See this: {@link org.apache.lens.server.api.metastore.DataCompletenessChecker}
   *
   * @param cubeql
   * @param cubeCol
   * @param alias
   * @param measureTag
   * @param tagToMeasureOrExprMap
   * @return
   */
  public static boolean processCubeColForDataCompleteness(CubeQueryContext cubeql, String cubeCol, String alias,
    Set<String> measureTag, Map<String, String> tagToMeasureOrExprMap) {
    CubeMeasure column = cubeql.getCube().getMeasureByName(cubeCol);
    if (column != null && column.getTags() != null) {
      String dataCompletenessTag = column.getTags().get(MetastoreConstants.MEASURE_DATACOMPLETENESS_TAG);
      //Checking if dataCompletenessTag is set for queried measure
      if (dataCompletenessTag != null) {
        measureTag.add(dataCompletenessTag);
        String value = tagToMeasureOrExprMap.get(dataCompletenessTag);
        if (value == null) {
          tagToMeasureOrExprMap.put(dataCompletenessTag, alias);
        } else {
          value = value.concat(",").concat(alias);
          tagToMeasureOrExprMap.put(dataCompletenessTag, value);
        }
        return true;
      }
    }
    return false;
  }

  /**
   * This method extracts all the columns used in expressions (used in query) and evaluates each
   * column separately for completeness
   *
   * @param cubeql
   * @param measureTag
   * @param tagToMeasureOrExprMap
   */
  public static void processExpressionsForCompleteness(CubeQueryContext cubeql, Set<String> measureTag,
    Map<String, String> tagToMeasureOrExprMap) {
    boolean isExprProcessed;
    String cubeAlias = cubeql.getAliasForTableName(cubeql.getCube().getName());
    for (String expr : cubeql.getQueriedExprsWithMeasures()) {
      isExprProcessed = false;
      for (ExpressionResolver.ExprSpecContext esc : cubeql.getExprCtx().getExpressionContext(expr, cubeAlias)
        .getAllExprs()) {
        if (esc.getTblAliasToColumns().get(cubeAlias) != null) {
          for (String cubeCol : esc.getTblAliasToColumns().get(cubeAlias)) {
            if (processCubeColForDataCompleteness(cubeql, cubeCol, expr, measureTag, tagToMeasureOrExprMap)) {
              /* This is done to associate the expression with one of the dataCompletenessTag for the measures.
              So, even if the expression is composed of measures with different dataCompletenessTags, we will be
              determining the dataCompleteness from one of the measure and this expression is grouped with the
              other queried measures that have the same dataCompletenessTag. */
              isExprProcessed = true;
              break;
            }
          }
        }
        if (isExprProcessed) {
          break;
        }
      }
    }
  }
}

