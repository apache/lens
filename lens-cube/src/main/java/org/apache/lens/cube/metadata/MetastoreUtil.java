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

package org.apache.lens.cube.metadata;

import static org.apache.lens.cube.metadata.MetastoreConstants.*;

import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

public class MetastoreUtil {
  private MetastoreUtil() {

  }

  public static final String getFactStorageTableName(String factName, String storageName) {
    return getStorageTableName(factName, Storage.getPrefix(storageName));
  }

  public static final String getDimStorageTableName(String dimName, String storageName) {
    return getStorageTableName(dimName, Storage.getPrefix(storageName));
  }

  public static final String getStorageTableName(String cubeTableName, String storagePrefix) {
    return storagePrefix + cubeTableName;
  }

  public static String getStorageClassKey(String name) {
    return getStorageEntityPrefix(name) + CLASS_SFX;
  }

  public static final String getStorageEntityPrefix(String storageName) {
    return STORAGE_ENTITY_PFX + storageName.toLowerCase();
  }

  // //////////////////////////
  // Dimension properties ///
  // /////////////////////////
  public static final String getDimPrefix(String dimName) {
    return DIMENSION_PFX + dimName.toLowerCase();
  }

  public static final String getDimAttributeListKey(String dimName) {
    return getDimPrefix(dimName) + ATTRIBUTES_LIST_SFX;
  }

  public static final String getDimTimedDimensionKey(String dimName) {
    return getDimPrefix(dimName) + TIMED_DIMENSION_SFX;
  }

  // ///////////////////////
  // Dimension attribute properties//
  // ///////////////////////
  public static String getDimensionKeyPrefix(String dimName) {
    return DIM_KEY_PFX + dimName.toLowerCase();
  }

  public static final String getDimensionClassPropertyKey(String dimName) {
    return getDimensionKeyPrefix(dimName) + CLASS_SFX;
  }

  public static String getInlineDimensionSizeKey(String name) {
    return getDimensionKeyPrefix(name) + INLINE_SIZE_SFX;
  }

  public static String getInlineDimensionValuesKey(String name) {
    return getDimensionKeyPrefix(name) + INLINE_VALUES_SFX;
  }

  public static final String getDimTypePropertyKey(String dimName) {
    return getDimensionKeyPrefix(dimName) + TYPE_SFX;
  }

  public static final String getDimNumOfDistinctValuesPropertyKey(String dimName) {
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

  public static final String getDimensionSrcReferenceKey(String dimName) {
    return getDimensionKeyPrefix(dimName) + DIM_REFERS_SFX;
  }

  public static final String getDimRefChainNameKey(String dimName) {
    return getDimensionKeyPrefix(dimName) + CHAIN_NAME_SFX;
  }

  public static final String getDimRefChainColumnKey(String dimName) {
    return getDimensionKeyPrefix(dimName) + CHAIN_REF_COLUMN_SFX;
  }

  public static String getDimUseAsJoinKey(String dimName) {
    return getDimensionKeyPrefix(dimName) + IS_JOIN_KEY_SFX;
  }

  public static final String getDimensionDestReference(String tableName, String columnName) {
    return tableName.toLowerCase() + TABLE_COLUMN_SEPERATOR + columnName.toLowerCase();
  }

  public static final String getReferencesString(List<TableReference> references) {
    String[] toks = new String[references.size()];

    for (int i = 0; i < references.size(); i++) {
      TableReference reference = references.get(i);
      toks[i] = reference.getDestTable() + TABLE_COLUMN_SEPERATOR + reference.getDestColumn();
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

  public static String getCubeColCostPropertyKey(String colName) {
    return getColumnKeyPrefix(colName) + COST_SFX;
  }

  public static String getCubeColDescriptionKey(String colName) {
    return getColumnKeyPrefix(colName) + DESC_SFX;
  }

  public static String getCubeColDisplayKey(String colName) {
    return getColumnKeyPrefix(colName) + DISPLAY_SFX;
  }

  public static final String getExprColumnKey(String colName) {
    return getColumnKeyPrefix(colName) + EXPR_SFX;
  }

  public static final String getExprTypePropertyKey(String colName) {
    return getColumnKeyPrefix(colName) + TYPE_SFX;
  }

  public static final String getExprEncodingPropertyKey(String colName) {
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
  public static final String getMeasurePrefix(String measureName) {
    return MEASURE_KEY_PFX + measureName.toLowerCase();
  }

  public static final String getMeasureClassPropertyKey(String measureName) {
    return getMeasurePrefix(measureName) + CLASS_SFX;
  }

  public static final String getMeasureUnitPropertyKey(String measureName) {
    return getMeasurePrefix(measureName) + UNIT_SFX;
  }

  public static final String getMeasureTypePropertyKey(String measureName) {
    return getMeasurePrefix(measureName) + TYPE_SFX;
  }

  public static final String getMeasureFormatPropertyKey(String measureName) {
    return getMeasurePrefix(measureName) + FORMATSTRING_SFX;
  }

  public static final String getMeasureAggrPropertyKey(String measureName) {
    return getMeasurePrefix(measureName) + AGGR_SFX;
  }

  public static final String getMeasureMinPropertyKey(String measureName) {
    return getMeasurePrefix(measureName) + MIN_SFX;
  }

  public static final String getMeasureMaxPropertyKey(String measureName) {
    return getMeasurePrefix(measureName) + MAX_SFX;
  }

  public static final String getExpressionListKey(String name) {
    return getBasePrefix(name) + EXPRESSIONS_LIST_SFX;
  }

  // //////////////////////////
  // Cube properties ///
  // /////////////////////////
  public static final String getBasePrefix(String base) {
    return BASE_KEY_PFX + base.toLowerCase();
  }

  public static final String getCubePrefix(String cubeName) {
    return CUBE_KEY_PFX + cubeName.toLowerCase();
  }

  public static final String getCubeMeasureListKey(String cubeName) {
    return getCubePrefix(cubeName) + MEASURES_LIST_SFX;
  }

  public static final String getCubeDimensionListKey(String cubeName) {
    return getCubePrefix(cubeName) + DIMENSIONS_LIST_SFX;
  }

  public static final String getCubeTimedDimensionListKey(String cubeName) {
    return getCubePrefix(cubeName) + TIMED_DIMENSIONS_LIST_SFX;
  }

  public static final String getCubeJoinChainListKey(String cubeName) {
    return getCubePrefix(cubeName) + JOIN_CHAIN_LIST_SFX;
  }

  public static final String getDimensionJoinChainListKey(String dimName) {
    return getDimPrefix(dimName) + JOIN_CHAIN_LIST_SFX;
  }

  public static final String getParentCubeNameKey(String cubeName) {
    return getCubePrefix(cubeName) + PARENT_CUBE_SFX;
  }

  public static final String getCubeTableKeyPrefix(String tableName) {
    return CUBE_TABLE_PFX + tableName.toLowerCase();
  }

  // //////////////////////////
  // Fact propertes ///
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

  public static String getValidColumnsKey(String name) {
    return getFactKeyPrefix(name) + VALID_COLUMNS_SFX;
  }

  public static String getCubeTableWeightKey(String name) {
    return getCubeTableKeyPrefix(name) + WEIGHT_KEY_SFX;
  }

  public static String getLatestPartTimestampKey(String partCol) {
    return MetastoreConstants.STORAGE_PFX + partCol + MetastoreConstants.LATEST_PART_TIMESTAMP_SFX;
  }

  // //////////////////////////
  // Utils ///
  // /////////////////////////
  public static <E extends Named> String getNamedStr(Collection<E> set) {
    if (set == null) {
      return "";
    }
    String sep = "";
    StringBuilder valueStr = new StringBuilder();
    Iterator<E> it = set.iterator();
    while (it.hasNext()) {
      valueStr.append(sep).append(it.next().getName());
      sep = ",";
    }
    return valueStr.toString();
  }

  static <E extends Named> List<String> getNamedStrs(Collection<E> set, int maxLength) {
    List<String> namedStrings = new ArrayList<String>();
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

  private static int maxParamLength = 3999;

  public static <E extends Named> void addNameStrings(Map<String, String> props, String key, Collection<E> set) {
    addNameStrings(props, key, set, maxParamLength);
  }

  static <E extends Named> void addNameStrings(Map<String, String> props, String key,
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

  public static Set<String> getColumnNames(AbstractCubeTable table) {
    List<FieldSchema> fields = table.getColumns();
    Set<String> columns = new HashSet<String>(fields.size());
    for (FieldSchema f : fields) {
      columns.add(f.getName().toLowerCase());
    }
    return columns;
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

  public static String getPartitoinTimelineCachePresenceKey() {
    return STORAGE_PFX + PARTITION_TIMELINE_CACHE + "present";
  }
}
