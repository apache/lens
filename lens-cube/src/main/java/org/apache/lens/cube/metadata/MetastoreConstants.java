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

public final class MetastoreConstants {
  private MetastoreConstants() {

  }

  public static final String TABLE_TYPE_KEY = "cube.table.type";
  public static final String CUBE_TABLE_PFX = "cube.table.";
  public static final String WEIGHT_KEY_SFX = ".weight";

  public static final String BASE_KEY_PFX = "base.";
  public static final String EXPRESSIONS_LIST_SFX = ".expressions.list";

  // Cube constants
  public static final String CUBE_KEY_PFX = "cube.";
  public static final String MEASURES_LIST_SFX = ".measures.list";
  public static final String DIMENSIONS_LIST_SFX = ".dimensions.list";
  public static final String JOIN_CHAIN_LIST_SFX = ".joinchains.list";
  public static final String TIMED_DIMENSIONS_LIST_SFX = ".timed.dimensions.list";
  public static final String PARENT_CUBE_SFX = ".parent.cube";
  public static final String CUBE_ALL_FIELDS_QUERIABLE = "cube.allfields.queriable";
  public static final String CUBE_ABSOLUTE_START_TIME = "cube.absolute.start.time";
  public static final String CUBE_RELATIVE_START_TIME = "cube.relative.start.time";
  public static final String CUBE_ABSOLUTE_END_TIME = "cube.absolute.end.time";
  public static final String CUBE_RELATIVE_END_TIME = "cube.relative.end.time";

  // Uber dimension constants
  public static final String DIMENSION_PFX = "dimension.";
  public static final String ATTRIBUTES_LIST_SFX = ".attributes.list";
  public static final String PARTCOLS_SFX = ".part.cols";
  public static final String TIMED_DIMENSION_SFX = ".timed.dimension";

  // fact constants
  public static final String FACT_KEY_PFX = "cube.fact.";
  public static final String UPDATE_PERIOD_SFX = ".updateperiods";
  public static final String CUBE_NAME_SFX = ".cubename";
  public static final String SOURCE_NAME_SFX = ".source";
  public static final String VALID_COLUMNS_SFX = ".valid.columns";
  public static final String RESTRICTED_COLUMNS_SFX = ".restricted.columns";
  public static final String FACT_AGGREGATED_PROPERTY = "cube.fact.is.aggregated";
  public static final String FACT_ABSOLUTE_START_TIME = "cube.fact.absolute.start.time";
  public static final String FACT_RELATIVE_START_TIME = "cube.fact.relative.start.time";
  public static final String FACT_ABSOLUTE_END_TIME = "cube.fact.absolute.end.time";
  public static final String FACT_RELATIVE_END_TIME = "cube.fact.relative.end.time";
  public static final String FACT_COL_START_TIME_PFX = "cube.fact.col.start.time.";
  public static final String FACT_COL_END_TIME_PFX = "cube.fact.col.end.time.";
  public static final String FACT_DATA_COMPLETENESS_TAG = "cube.fact.datacompleteness.tag";
  public static final String VIRTUAL_FACT_FILTER = "cube.fact.query.where.filter";

  // Segmentation constants
  public static final String SEGMENTATION_KEY_PFX = "cube.segmentation.internal.";
  public static final String SEGMENTATION_ABSOLUTE_START_TIME = "cube.segmentation.absolute.start.time";
  public static final String SEGMENTATION_RELATIVE_START_TIME = "cube.segmentation.relative.start.time";
  public static final String SEGMENTATION_ABSOLUTE_END_TIME = "cube.segmentation.absolute.end.time";
  public static final String SEGMENTATION_RELATIVE_END_TIME = "cube.segmentation.relative.end.time";
  public static final String SEGMENTATION_CUBE_SEGMENT_SFX = ".segments";
  public static final String SEGMENT_PROP_SFX = ".props.";

  // dim table constants
  // TODO: remove this and move to "dimtable."
  public static final String DIM_TBL_PFX = "dimtble.";
  public static final String DIM_TABLE_PFX = "dimtable.";
  public static final String DUMP_PERIOD_SFX = ".dumpperiod";
  public static final String STORAGE_LIST_SFX = ".storages";
  public static final String DIM_NAME_SFX = ".dim.name";

  // column constants
  public static final String COL_PFX = "cube.col.";
  public static final String TYPE_SFX = ".type";
  public static final String BASE64_SFX = ".base64";
  public static final String START_TIME_SFX = ".starttime";
  public static final String END_TIME_SFX = ".endtime";
  public static final String COST_SFX = ".cost";
  public static final String DESC_SFX = ".description";
  public static final String DISPLAY_SFX = ".displaystring";
  public static final String NUM_DISTINCT_VALUES = ".num.distinct.values";
  public static final String TAGS_PFX = ".tags.";

  // measure constants
  public static final String MEASURE_KEY_PFX = "cube.measure.";
  public static final String UNIT_SFX = ".unit";
  public static final String AGGR_SFX = ".aggregate";
  public static final String MIN_SFX = ".min";
  public static final String MAX_SFX = ".max";
  public static final String EXPR_SFX = ".expr";
  public static final String FORMATSTRING_SFX = ".format";
  public static final String MEASURE_DATACOMPLETENESS_TAG = "cube.measure.datacompleteness.tag";

  // dimension constants
  public static final String DIM_KEY_PFX = "cube.dimension.";
  public static final String DIM_REFERS_SFX = ".refers";
  public static final String CHAIN_NAME_SFX = ".chain.name";
  public static final String CHAIN_REF_COLUMN_SFX = ".chain.column.name";
  public static final String IS_JOIN_KEY_SFX = ".isjoinkey";
  public static final String TABLE_COLUMN_SEPERATOR = ".";
  public static final String INLINE_SIZE_SFX = ".inline.size";
  public static final String INLINE_VALUES_SFX = ".inline.values";
  public static final String HIERARCHY_SFX = ".hierarchy.";
  public static final String CLASS_SFX = ".class";
  public static final String METASTORE_ENABLE_CACHING = "cube.metastore.enable.cache";

  // join chain constants
  public static final String JOIN_CHAIN_KEY = "joinchain.";
  public static final String NUM_CHAINS_SFX = ".numchains";
  public static final String FULL_CHAIN_KEY = ".fullchain.";

  // storage constants
  public static final String STORAGE_ENTITY_PFX = "storage.";
  public static final String STORAGE_PFX = "cube.storagetable.";
  public static final String PARTITION_TIMELINE_CACHE = "partition.timeline.cache.";
  public static final String STORAGE_CLASS = "storage.class";
  public static final String TIME_PART_COLUMNS = "cube.storagetable.time.partcols";
  public static final String LATEST_PART_TIMESTAMP_SFX = ".latest.part.timestamp";
  public static final String PARTITION_UPDATE_PERIOD_SFX = ".partition.update.period";
  public static final String PARTITION_UPDATE_PERIOD = "cube.storagetable.partition.update.period";
  public static final String TIMEDIM_TO_PART_MAPPING_PFX = "cube.timedim.partition.";
  public static final String TIMEDIM_RELATION = "cube.timedim.relation.";
}
