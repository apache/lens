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

import java.util.Arrays;
import java.util.List;

import org.apache.lens.cube.metadata.UpdatePeriod;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
/**
 * Contains all configurations of cube query rewriting.
 */
public final class CubeQueryConfUtil {
  private CubeQueryConfUtil() {

  }

  public static final String STORAGE_TABLES_SFX = ".storagetables";
  public static final String UPDATE_PERIODS_SFX = ".updateperiods";
  public static final String FACT_TABLES_SFX = ".facttables";
  public static final String STORAGE_KEY_PFX = ".storage.";
  public static final String VALID_PFX = "lens.cube.query.valid.";
  public static final String VALID_FACT_PFX = VALID_PFX + "fact.";
  public static final String VALID_STORAGE_DIM_TABLES = "lens.cube.query.valid." + "dim.storgaetables";
  public static final String DRIVER_SUPPORTED_STORAGES = "lens.cube.query.driver." + "supported.storages";
  public static final String FAIL_QUERY_ON_PARTIAL_DATA = "lens.cube.query.fail.if.data.partial";
  public static final String RESOLVE_SEGMENTATIONS = "lens.cube.query.resolve.segmentations";
  public static final String NON_EXISTING_PARTITIONS = "lens.cube.query.nonexisting.partitions";
  public static final String QUERY_MAX_INTERVAL = "lens.cube.query.max.interval";
  public static final String PROCESS_TIME_PART_COL = "lens.cube.query.process.time" + ".partition.column";
  public static final String COMPLETENESS_CHECK_PART_COL = "lens.cube.query.completeness.check.partition.column";
  public static final String LOOK_AHEAD_PT_PARTS_PFX = "lens.cube.query.lookahead.ptparts.forinterval.";
  public static final String LOOK_AHEAD_TIME_PARTS_PFX = "lens.cube.query.lookahead.timeparts.forinterval.";
  public static final String ENABLE_GROUP_BY_TO_SELECT = "lens.cube.query.promote.groupby.toselect";
  public static final String ENABLE_SELECT_TO_GROUPBY = "lens.cube.query.promote.select.togroupby";
  public static final String ENABLE_ATTRFIELDS_ADD_DISTINCT = "lens.cube.query.enable.attrfields.add.distinct";
  public static final boolean DEFAULT_ATTR_FIELDS_ADD_DISTINCT = true;
  public static final String ENABLE_STORAGES_UNION = "lens.cube.query.enable.storages.union";
  public static final boolean DEFAULT_ENABLE_STORAGES_UNION = false;

  public static final String REPLACE_TIMEDIM_WITH_PART_COL = "lens.cube.query.replace.timedim";
  public static final boolean DEFAULT_MULTI_TABLE_SELECT = true;
  public static final int DEFAULT_LOOK_AHEAD_PT_PARTS = 1;
  public static final int DEFAULT_LOOK_AHEAD_TIME_PARTS = 1;
  public static final boolean DEFAULT_ENABLE_GROUP_BY_TO_SELECT = false;
  public static final boolean DEFAULT_ENABLE_SELECT_TO_GROUPBY = false;
  public static final boolean DEFAULT_REPLACE_TIMEDIM_WITH_PART_COL = true;

  public static String getLookAheadPTPartsKey(UpdatePeriod interval) {
    return LOOK_AHEAD_PT_PARTS_PFX + interval.name().toLowerCase();
  }

  public static String getLookAheadTimePartsKey(UpdatePeriod interval) {
    return LOOK_AHEAD_TIME_PARTS_PFX + interval.name().toLowerCase();
  }

  private static String getValidKeyCubePFX(String cubeName) {
    return VALID_PFX + cubeName.toLowerCase();
  }

  private static String getValidKeyFactPFX(String factName) {
    return VALID_FACT_PFX + factName.toLowerCase();
  }

  private static String getValidKeyStoragePFX(String factName, String storage) {
    return getValidKeyFactPFX(factName) + STORAGE_KEY_PFX + storage.toLowerCase();
  }

  public static String getValidFactTablesKey(String cubeName) {
    return getValidKeyCubePFX(cubeName) + FACT_TABLES_SFX;
  }

  public static String getValidStorageTablesKey(String factName) {
    return getValidKeyFactPFX(factName) + STORAGE_TABLES_SFX;
  }

  public static String getValidUpdatePeriodsKey(String fact, String storage) {
    return getValidKeyStoragePFX(fact, storage) + UPDATE_PERIODS_SFX;
  }

  public static List<String> getStringList(Configuration conf, String keyName) {
    String str = conf.get(keyName);
    List<String> list = StringUtils.isBlank(str) ? null : Arrays.asList(StringUtils.split(str.toLowerCase(), ","));
    return list;
  }

  public static final String DISABLE_AUTO_JOINS = "lens.cube.query.disable.auto.join";
  public static final boolean DEFAULT_DISABLE_AUTO_JOINS = true;
  public static final String JOIN_TYPE_KEY = "lens.cube.query.join.type";
  public static final String DISABLE_AGGREGATE_RESOLVER = "lens.cube.query.disable.aggregate.resolver";
  public static final boolean DEFAULT_DISABLE_AGGREGATE_RESOLVER = true;
  public static final String LIGHTEST_FACT_FIRST = "lens.cube.query.pick.lightest.fact.first";
  public static final boolean DEFAULT_LIGHTEST_FACT_FIRST = false;
  public static final String TIME_RANGE_WRITER_CLASS = "lens.cube.query.time.range.writer.class";
  public static final boolean DEFAULT_BETWEEN_ONLY_TIME_RANGE_WRITER = false;
  public static final String BETWEEN_ONLY_TIME_RANGE_WRITER = "lens.cube.query.between.only.time.range.writer";
  public static final String DEFAULT_START_BOUND_TYPE = "CLOSED";
  public static final String DEFAULT_END_BOUND_TYPE = "CLOSED";
  public static final String START_DATE_BOUND_TYPE = "lens.cube.query.time.range.writer.start.bound.type";
  public static final String END_DATE_BOUND_TYPE = "lens.cube.query.time.range.writer.end.bound.type";
  public static final Class<? extends TimeRangeWriter> DEFAULT_TIME_RANGE_WRITER = ORTimeRangeWriter.class
    .asSubclass(TimeRangeWriter.class);
  public static final String PART_WHERE_CLAUSE_DATE_FORMAT = "lens.cube.query.partition.where.clause.format";
  public static final String ENABLE_FLATTENING_FOR_BRIDGETABLES = "lens.cube.query.enable.flattening.bridge.tables";
  public static final boolean DEFAULT_ENABLE_FLATTENING_FOR_BRIDGETABLES = false;
  public static final String BRIDGE_TABLE_FIELD_AGGREGATOR = "lens.cube.query.bridge.table.field.aggregator";
  public static final String DEFAULT_BRIDGE_TABLE_FIELD_AGGREGATOR = "collect_set";
  public static final String DO_FLATTENING_OF_BRIDGE_TABLE_EARLY =
    "lens.cube.query.flatten.bridge.tables.early";
  public static final boolean DEFAULT_DO_FLATTENING_OF_BRIDGE_TABLE_EARLY = false;
  public static final String BRIDGE_TABLE_FIELD_ARRAY_FILTER = "lens.cube.query.bridge.table.field.array.filter";
  public static final String DEFAULT_BRIDGE_TABLE_FIELD_ARRAY_FILTER = "array_contains";
  public static final String REWRITE_DIM_FILTER_TO_FACT_FILTER = "lens.cube.query.rewrite.dim.filter.to.fact.filter";
  public static final boolean DEFAULT_REWRITE_DIM_FILTER_TO_FACT_FILTER = false;
  public static final String COMPLETENESS_THRESHOLD = "lens.cube.query.completeness.threshold";
  public static final float DEFAULT_COMPLETENESS_THRESHOLD = 100f;
}
