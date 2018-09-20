/*
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

package org.apache.lens.cube.metadata;

import static org.apache.lens.cube.error.LensCubeErrorCode.EXPRESSION_NOT_PARSABLE;
import static org.apache.lens.cube.metadata.MetastoreConstants.*;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseUtils;

import org.antlr.runtime.CommonToken;

import com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetastoreUtil {
  private MetastoreUtil() {

  }

  public static String getFactOrDimtableStorageTableName(String factName, String storageName) {
    return getStorageTableName(factName, Storage.getPrefix(storageName));
  }

  public static String getStorageTableName(String cubeTableName, String storagePrefix) {
    return (storagePrefix + cubeTableName).toLowerCase();
  }

  public static String getStorageClassKey(String name) {
    return getStorageEntityPrefix(name) + CLASS_SFX;
  }

  public static String getStorageEntityPrefix(String storageName) {
    return STORAGE_ENTITY_PFX + storageName.toLowerCase();
  }

  // //////////////////////////
  // Dimension properties ///
  // /////////////////////////
  public static String getDimPrefix(String dimName) {
    return DIMENSION_PFX + dimName.toLowerCase();
  }

  public static String getDimAttributeListKey(String dimName) {
    return getDimPrefix(dimName) + ATTRIBUTES_LIST_SFX;
  }

  public static String getDimTablePartsKey(String dimtableName) {
    return DIM_TABLE_PFX + dimtableName + PARTCOLS_SFX;
  }

  public static String getDimTimedDimensionKey(String dimName) {
    return getDimPrefix(dimName) + TIMED_DIMENSION_SFX;
  }

  // ///////////////////////
  // Dimension attribute properties//
  // ///////////////////////
  public static String getDimensionKeyPrefix(String dimName) {
    return DIM_KEY_PFX + dimName.toLowerCase();
  }

  public static String getDimensionClassPropertyKey(String dimName) {
    return getDimensionKeyPrefix(dimName) + CLASS_SFX;
  }

  public static String getInlineDimensionValuesKey(String name) {
    return getDimensionKeyPrefix(name) + INLINE_VALUES_SFX;
  }

  public static String getDimTypePropertyKey(String dimName) {
    return getDimensionKeyPrefix(dimName) + TYPE_SFX;
  }

  public static String getDimNumOfDistinctValuesPropertyKey(String dimName) {
    return getDimensionKeyPrefix(dimName) + NUM_DISTINCT_VALUES;
  }

  public static String getHierachyElementKeyPFX(String dimName) {
    return getDimensionKeyPrefix(dimName) + HIERARCHY_SFX;
  }

  public static String getHierachyElementKeyName(String dimName, int index) {
    return getHierachyElementKeyPFX(dimName) + index;
  }

  public static Integer getHierachyElementIndex(String dimName, String param) {
    return Integer.parseInt(param.substring(getHierachyElementKeyPFX(dimName).length()));
  }

  public static String getDimensionSrcReferenceKey(String dimName) {
    return getDimensionKeyPrefix(dimName) + DIM_REFERS_SFX;
  }

  public static String getDimRefChainNameKey(String dimName) {
    return getDimensionKeyPrefix(dimName) + CHAIN_NAME_SFX;
  }

  public static String getDimRefChainColumnKey(String dimName) {
    return getDimensionKeyPrefix(dimName) + CHAIN_REF_COLUMN_SFX;
  }

  public static String getDimUseAsJoinKey(String dimName) {
    return getDimensionKeyPrefix(dimName) + IS_JOIN_KEY_SFX;
  }

  public static String getReferencesString(List<TableReference> references) {
    String[] toks = new String[references.size()];

    for (int i = 0; i < references.size(); i++) {
      TableReference reference = references.get(i);
      toks[i] = reference.getDestTable() + TABLE_COLUMN_SEPERATOR + reference.getDestColumn();
      toks[i] += TABLE_COLUMN_SEPERATOR + reference.isMapsToMany();
    }

    return StringUtils.join(toks, ',');
  }

  // ////////////////////////
  // Column properties //
  // ////////////////////////
  public static String getColumnKeyPrefix(String colName) {
    return COL_PFX + colName.toLowerCase();
  }

  public static String getCubeColStartTimePropertyKey(String colName) {
    return getColumnKeyPrefix(colName) + START_TIME_SFX;
  }

  public static String getCubeColEndTimePropertyKey(String colName) {
    return getColumnKeyPrefix(colName) + END_TIME_SFX;
  }

  public static String getStoragetableStartTimesKey() {
    return STORAGE_PFX + "start.times";
  }

  public static String getStoragetableEndTimesKey() {
    return STORAGE_PFX + "end.times";
  }

  public static String getCubeColCostPropertyKey(String colName) {
    return getColumnKeyPrefix(colName) + COST_SFX;
  }

  public static String getCubeColDescriptionKey(String colName) {
    return getColumnKeyPrefix(colName) + DESC_SFX;
  }

  public static String getCubeColDisplayKey(String colName) {
    return getColumnKeyPrefix(colName) + DISPLAY_SFX;
  }

  public static String getCubeColTagKey(String colName) {
    return getColumnKeyPrefix(colName) + TAGS_PFX;
  }

  public static String getExprColumnKey(String colName) {
    return getColumnKeyPrefix(colName) + EXPR_SFX;
  }

  public static String getExprTypePropertyKey(String colName) {
    return getColumnKeyPrefix(colName) + TYPE_SFX;
  }

  public static String getExprEncodingPropertyKey(String colName) {
    return getExprColumnKey(colName) + BASE64_SFX;
  }

  ////////////////////////////
  // Join chain properties  //
  ////////////////////////////
  public static String getCubeJoinChainKey(String colName) {
    return CUBE_KEY_PFX + JOIN_CHAIN_KEY + colName.toLowerCase();
  }

  public static String getCubeJoinChainNumChainsKey(String colName) {
    return getCubeJoinChainKey(colName) + NUM_CHAINS_SFX;
  }

  public static String getCubeJoinChainFullChainKey(String colName, int index) {
    return getCubeJoinChainKey(colName) + FULL_CHAIN_KEY + index;
  }

  public static String getCubeJoinChainDescriptionKey(String colName) {
    return getCubeJoinChainKey(colName) + DESC_SFX;
  }

  public static String getCubeJoinChainDisplayKey(String colName) {
    return getCubeJoinChainKey(colName) + DISPLAY_SFX;
  }

  public static String getDimensionJoinChainKey(String colName) {
    return DIMENSION_PFX + JOIN_CHAIN_KEY + colName.toLowerCase();
  }

  public static String getDimensionJoinChainNumChainsKey(String colName) {
    return getDimensionJoinChainKey(colName) + NUM_CHAINS_SFX;
  }

  public static String getDimensionJoinChainFullChainKey(String colName, int index) {
    return getDimensionJoinChainKey(colName) + FULL_CHAIN_KEY + index;
  }

  public static String getDimensionJoinChainDescriptionKey(String colName) {
    return getDimensionJoinChainKey(colName) + DESC_SFX;
  }

  public static String getDimensionJoinChainDisplayKey(String colName) {
    return getDimensionJoinChainKey(colName) + DISPLAY_SFX;
  }


  // ////////////////////////
  // Dimension table properties //
  // ////////////////////////
  public static String getDimensionTablePrefix(String dimTblName) {
    return DIM_TBL_PFX + dimTblName.toLowerCase();
  }

  public static String getDimensionDumpPeriodKey(String dimTblName, String storage) {
    return getDimensionTablePrefix(dimTblName) + "." + storage.toLowerCase() + DUMP_PERIOD_SFX;
  }

  public static String getDimensionStorageListKey(String dimTblName) {
    return getDimensionTablePrefix(dimTblName) + STORAGE_LIST_SFX;
  }

  public static String getDimNameKey(String dimTblName) {
    return getDimensionTablePrefix(dimTblName) + DIM_NAME_SFX;
  }

  // //////////////////////////
  // Measure properties ///
  // /////////////////////////
  public static String getMeasurePrefix(String measureName) {
    return MEASURE_KEY_PFX + measureName.toLowerCase();
  }

  public static String getMeasureClassPropertyKey(String measureName) {
    return getMeasurePrefix(measureName) + CLASS_SFX;
  }

  public static String getMeasureUnitPropertyKey(String measureName) {
    return getMeasurePrefix(measureName) + UNIT_SFX;
  }

  public static String getMeasureTypePropertyKey(String measureName) {
    return getMeasurePrefix(measureName) + TYPE_SFX;
  }

  public static String getMeasureFormatPropertyKey(String measureName) {
    return getMeasurePrefix(measureName) + FORMATSTRING_SFX;
  }

  public static String getMeasureAggrPropertyKey(String measureName) {
    return getMeasurePrefix(measureName) + AGGR_SFX;
  }

  public static String getMeasureMinPropertyKey(String measureName) {
    return getMeasurePrefix(measureName) + MIN_SFX;
  }

  public static String getMeasureMaxPropertyKey(String measureName) {
    return getMeasurePrefix(measureName) + MAX_SFX;
  }

  public static String getExpressionListKey(String name) {
    return getBasePrefix(name) + EXPRESSIONS_LIST_SFX;
  }

  // //////////////////////////
  // Cube properties ///
  // /////////////////////////
  public static String getBasePrefix(String base) {
    return BASE_KEY_PFX + base.toLowerCase();
  }

  public static String getCubePrefix(String cubeName) {
    return CUBE_KEY_PFX + cubeName.toLowerCase();
  }

  public static String getCubeMeasureListKey(String cubeName) {
    return getCubePrefix(cubeName) + MEASURES_LIST_SFX;
  }

  public static String getCubeDimensionListKey(String cubeName) {
    return getCubePrefix(cubeName) + DIMENSIONS_LIST_SFX;
  }

  public static String getCubeTimedDimensionListKey(String cubeName) {
    return getCubePrefix(cubeName) + TIMED_DIMENSIONS_LIST_SFX;
  }

  public static String getCubeJoinChainListKey(String cubeName) {
    return getCubePrefix(cubeName) + JOIN_CHAIN_LIST_SFX;
  }

  public static String getDimensionJoinChainListKey(String dimName) {
    return getDimPrefix(dimName) + JOIN_CHAIN_LIST_SFX;
  }

  public static String getParentCubeNameKey(String cubeName) {
    return getCubePrefix(cubeName) + PARENT_CUBE_SFX;
  }

  public static String getCubeTableKeyPrefix(String tableName) {
    return CUBE_TABLE_PFX + tableName.toLowerCase();
  }

  // //////////////////////////
  // Fact properties ///
  // /////////////////////////
  public static String getFactStorageListKey(String name) {
    return getFactKeyPrefix(name) + STORAGE_LIST_SFX;
  }

  public static String getFactKeyPrefix(String factName) {
    return FACT_KEY_PFX + factName.toLowerCase();
  }

  public static String getFactUpdatePeriodKey(String name, String storage) {
    return getFactKeyPrefix(name) + "." + storage.toLowerCase() + UPDATE_PERIOD_SFX;
  }

  public static String getFactCubeNameKey(String name) {
    return getFactKeyPrefix(name) + CUBE_NAME_SFX;
  }

  public static String getSourceFactNameKey(String name) {
    return getFactKeyPrefix(name) + SOURCE_NAME_SFX;
  }

  public static String getValidColumnsKey(String name) {
    return getFactKeyPrefix(name) + VALID_COLUMNS_SFX;
  }

  public static String getPartitionColumnKey(String name, String storage) {
    return getFactKeyPrefix(name) + "." + storage + PARTCOLS_SFX;
  }

  public static String getRestrictedColumnsKey(String name) {
    return getCubePrefix(name) + RESTRICTED_COLUMNS_SFX;
  }

  public static String getCubeTableWeightKey(String name) {
    return getCubeTableKeyPrefix(name) + WEIGHT_KEY_SFX;
  }

  public static String getLatestPartTimestampKey(String partCol) {
    return STORAGE_PFX + partCol + LATEST_PART_TIMESTAMP_SFX;
  }

  // //////////////////////////
  // Segmentation propertes ///
  // /////////////////////////

  public static String getSegmentationKeyPrefix(String segName) {
    return SEGMENTATION_KEY_PFX + segName.toLowerCase();
  }

  public static String getSegmentationCubeNameKey(String name) {
    return getSegmentationKeyPrefix(name) + CUBE_NAME_SFX;
  }

  public static String getSegmentsListKey(String name) {
    return getSegmentationKeyPrefix(name) + SEGMENTATION_CUBE_SEGMENT_SFX;
  }

  public static String getSegmentPropertyKey(String segName) {
    return getSegmentationKeyPrefix(segName) + SEGMENT_PROP_SFX;
  }

  // //////////////////////////
  // Utils ///
  // /////////////////////////

  public static Date getDateFromProperty(String prop, boolean relative, boolean start) {
    try {
      if (StringUtils.isNotBlank(prop)) {
        if (relative) {
          return DateUtil.resolveRelativeDate(prop, new Date());
        } else {
          return DateUtil.resolveAbsoluteDate(prop);
        }
      }
    } catch (LensException e) {
      log.error("unable to parse {} {} date: {}", relative ? "relative" : "absolute", start ? "start" : "end", prop);
    }
    return start ? DateUtil.MIN_DATE : DateUtil.MAX_DATE;
  }


  public static <E extends Named> String getNamedStr(Collection<E> set) {
    if (set == null) {
      return "";
    }
    String sep = "";
    StringBuilder valueStr = new StringBuilder();
    for (E aSet : set) {
      valueStr.append(sep).append(aSet.getName());
      sep = ",";
    }
    return valueStr.toString();
  }

  static <E extends Named> List<String> getNamedStrs(Collection<E> set, int maxLength) {
    List<String> namedStrings = new ArrayList<>();
    if (set == null || set.isEmpty()) {
      return namedStrings;
    }
    StringBuilder valueStr = new StringBuilder();
    Iterator<E> it = set.iterator();
    for (int i = 0; i < (set.size() - 1); i++) {
      E next = it.next();
      if (valueStr.length() + next.getName().length() >= maxLength) {
        namedStrings.add(valueStr.toString());
        valueStr.setLength(0);
      }
      valueStr.append(next.getName());
      valueStr.append(",");
    }
    E next = it.next();
    if (valueStr.length() + next.getName().length() >= maxLength) {
      namedStrings.add(valueStr.toString());
      valueStr.setLength(0);
    }
    valueStr.append(next.getName());
    namedStrings.add(valueStr.toString());
    return namedStrings;
  }

  private static final int MAX_PARAM_LENGTH = 3999;

  public static Set<Named> getNamedSetFromStringSet(Set<String> strings) {
    Set<Named> nameds = Sets.newHashSet();
    for (final String s : strings) {
      nameds.add(new Named() {
        @Override
        public String getName() {
          return s;
        }
      });
    }
    return nameds;
  }

  public static <E extends Named> void addNameStrings(Map<String, String> props, String key, Collection<E> set) {
    addNameStrings(props, key, set, MAX_PARAM_LENGTH);
  }

  public static <E extends Named> void addNameStrings(Map<String, String> props, String key,
    Collection<E> set, int maxLength) {
    List<String> namedStrings = getNamedStrs(set, maxLength);
    props.put(key + ".size", String.valueOf(namedStrings.size()));
    for (int i = 0; i < namedStrings.size(); i++) {
      props.put(key + i, namedStrings.get(i));
    }
  }

  public static String getNamedStringValue(Map<String, String> props, String key) {
    if (props.containsKey(key + ".size")) {
      int size = Integer.parseInt(props.get(key + ".size"));
      StringBuilder valueStr = new StringBuilder();
      for (int i = 0; i < size; i++) {
        valueStr.append(props.get(key + i));
      }
      return valueStr.toString();
    } else if (props.containsKey(key)) {
      return props.get(key);
    } else {
      return null;
    }
  }

  public static String getObjectStr(Collection<?> set) {
    if (set == null || set.isEmpty()) {
      return "";
    }
    StringBuilder valueStr = new StringBuilder();
    Iterator<?> it = set.iterator();
    for (int i = 0; i < (set.size() - 1); i++) {
      valueStr.append(it.next().toString());
      valueStr.append(",");
    }
    valueStr.append(it.next().toString());
    return valueStr.toString();
  }

  public static String getStr(Collection<String> set) {
    if (set == null || set.isEmpty()) {
      return "";
    }
    StringBuilder valueStr = new StringBuilder();
    Iterator<String> it = set.iterator();
    for (int i = 0; i < (set.size() - 1); i++) {
      valueStr.append(it.next());
      valueStr.append(",");
    }
    valueStr.append(it.next());
    return valueStr.toString();
  }

  public static void addColumnNames(CubeDimAttribute dim, Set<String> cols) {
    if (dim instanceof HierarchicalDimAttribute) {
      HierarchicalDimAttribute h = (HierarchicalDimAttribute) dim;
      for (CubeDimAttribute d : h.getHierarchy()) {
        addColumnNames(d, cols);
      }
    } else {
      cols.add(dim.getName().toLowerCase());
    }
  }

  public static String getPartitionInfoKeyPrefix(UpdatePeriod updatePeriod, String partCol) {
    return STORAGE_PFX + PARTITION_TIMELINE_CACHE + updatePeriod.getName() + "." + partCol + ".";
  }

  public static String getPartitionTimelineStorageClassKey(UpdatePeriod updatePeriod, String partCol) {
    return getPartitionInfoKeyPrefix(updatePeriod, partCol) + STORAGE_CLASS;
  }

  public static String getPartitionTimelineCachePresenceKey() {
    return STORAGE_PFX + PARTITION_TIMELINE_CACHE + "present";
  }

  public static void filterPartitionsByUpdatePeriod(List<Partition> partitions, UpdatePeriod updatePeriod) {
    Iterator<Partition> iter = partitions.iterator();
    while (iter.hasNext()) {
      Partition part = iter.next();
      if (!UpdatePeriod.valueOf(part.getParameters().get(PARTITION_UPDATE_PERIOD)).equals(updatePeriod)) {
        iter.remove();
      }
    }
  }

  public static List<Partition> filterPartitionsByNonTimeParts(List<Partition> partitions,
    Map<String, String> nonTimePartSpec,
    String latestPartCol) {
    ListIterator<Partition> iter = partitions.listIterator();
    while (iter.hasNext()) {
      Partition part = iter.next();
      boolean ignore = false;
      for (Map.Entry<String, String> entry1 : part.getSpec().entrySet()) {
        if ((nonTimePartSpec == null || !nonTimePartSpec.containsKey(entry1.getKey()))
          && !entry1.getKey().equals(latestPartCol)) {
          try {
            UpdatePeriod.valueOf(part.getParameters().get(PARTITION_UPDATE_PERIOD))
              .format()
              .parse(entry1.getValue());
          } catch (ParseException e) {
            ignore = true;
          }
        }
      }
      if (ignore) {
        iter.remove();
      }
    }
    return partitions;
  }

  public static Date getLatestTimeStampFromPartition(Partition part, String partCol) throws HiveException {
    if (part != null) {
      String latestTimeStampStr = part.getParameters().get(MetastoreUtil.getLatestPartTimestampKey(partCol));
      String latestPartUpdatePeriod = part.getParameters().get(PARTITION_UPDATE_PERIOD);
      UpdatePeriod latestUpdatePeriod = UpdatePeriod.valueOf(latestPartUpdatePeriod.toUpperCase());
      try {
        return latestUpdatePeriod.parse(latestTimeStampStr);
      } catch (ParseException e) {
        throw new HiveException(e);
      }
    }
    return null;
  }

  static ASTNode parseExpr(String expr) throws LensException {
    ParseDriver driver = new ParseDriver();
    ASTNode tree;
    try {
      tree = driver.parseExpression(expr);
    } catch (org.apache.hadoop.hive.ql.parse.ParseException e) {
      throw new LensException(EXPRESSION_NOT_PARSABLE.getLensErrorInfo(), e, e.getMessage(), expr);
    }
    return ParseUtils.findRootNonNullToken(tree);
  }

  public static ASTNode copyAST(ASTNode original) {
    return copyAST(original, x -> Pair.of(new ASTNode(x), true));
  }

  public static ASTNode copyAST(ASTNode original,
    Function<ASTNode, Pair<ASTNode, Boolean>> overrideCopyFunction) {
    Pair<ASTNode, Boolean> copy1 = overrideCopyFunction.apply(original);
    ASTNode copy = copy1.getLeft();
    if (copy1.getRight()) {
      if (original.getChildren() != null) {
        for (Node o : original.getChildren()) {
          copy.addChild(copyAST((ASTNode) o, overrideCopyFunction));
        }
      }
    }
    return copy;
  }

  public static String getUpdatePeriodStoragePrefixKey(String factTableName, String storageName, String updatePeriod) {
    return MetastoreUtil.getFactKeyPrefix(factTableName) + "." + storageName + "." + updatePeriod;
  }
  public static ASTNode getStringLiteralAST(String literal) {
    return new ASTNode(new CommonToken(HiveParser.StringLiteral, literal));
  }
}
