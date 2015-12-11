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

import static org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode.*;

import java.util.*;

import org.apache.lens.cube.metadata.TimeRange;

import org.codehaus.jackson.annotate.JsonWriteNullProperties;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Contains the cause why a candidate table is not picked for answering the query
 */
// no args constructor Needed for jackson. SUSPEND CHECKSTYLE CHECK HideUtilityClassConstructorCheck
@JsonWriteNullProperties(false)
@Data
@NoArgsConstructor
public class CandidateTablePruneCause {

  public enum CandidateTablePruneCode {
    // other fact set element is removed
    ELEMENT_IN_SET_PRUNED("Other candidate from measure covering set is pruned"),
    FACT_NOT_AVAILABLE_IN_RANGE("No facts available for all of these time ranges: %s") {
      @Override
      Object[] getFormatPlaceholders(Set<CandidateTablePruneCause> causes) {
        Set<TimeRange> allRanges = Sets.newHashSet();
        for (CandidateTablePruneCause cause : causes) {
          allRanges.addAll(cause.getInvalidRanges());
        }
        return new Object[]{
          allRanges.toString(),
        };
      }
    },
    // least weight not satisfied
    MORE_WEIGHT("Picked table had more weight than minimum."),
    // partial data is enabled, another fact has more data.
    LESS_DATA("Picked table has less data than the maximum"),
    // cube table has more partitions
    MORE_PARTITIONS("Picked table has more partitions than minimum"),
    // invalid cube table
    INVALID("Invalid cube table provided in query"),
    // expression is not evaluable in the candidate
    EXPRESSION_NOT_EVALUABLE("%s expressions not evaluable") {
      Object[] getFormatPlaceholders(Set<CandidateTablePruneCause> causes) {
        List<String> columns = new ArrayList<String>();
        for (CandidateTablePruneCause cause : causes) {
          columns.addAll(cause.getMissingExpressions());
        }
        return new String[]{columns.toString()};
      }
    },
    // candidate table tries to get denormalized field from dimension and the
    // referred dimension is invalid.
    INVALID_DENORM_TABLE("Referred dimension is invalid in one of the candidate tables"),
    // column not valid in cube table
    COLUMN_NOT_VALID("Column not valid in cube table"),
    // column not found in cube table
    COLUMN_NOT_FOUND("%s are not %s") {
      Object[] getFormatPlaceholders(Set<CandidateTablePruneCause> causes) {
        if (causes.size() == 1) {
          return new String[]{
            "Columns " + causes.iterator().next().getMissingColumns(),
            "present in any table",
          };
        } else {
          List<List<String>> columnSets = new ArrayList<List<String>>();
          for (CandidateTablePruneCause cause : causes) {
            columnSets.add(cause.getMissingColumns());
          }
          return new String[]{
            "Column Sets: " + columnSets,
            "queriable together",
          };
        }
      }
    },
    // missing storage tables for cube table
    MISSING_STORAGES("Missing storage tables for the cube table"),
    // no candidate storges for cube table, storage cause will have why each
    // storage is not a candidate
    NO_CANDIDATE_STORAGES("No candidate storages for any table"),
    // time dimension not supported. Either directly or indirectly.
    TIMEDIM_NOT_SUPPORTED("Queried data not available for time dimensions: %s") {
      @Override
      Object[] getFormatPlaceholders(Set<CandidateTablePruneCause> causes) {
        Set<String> dims = Sets.newHashSet();
        for(CandidateTablePruneCause cause: causes){
          dims.addAll(cause.getUnsupportedTimeDims());
        }
        return new Object[]{
          dims.toString(),
        };
      }
    },
    NO_FACT_UPDATE_PERIODS_FOR_GIVEN_RANGE("No fact update periods for given range"),
    NO_COLUMN_PART_OF_A_JOIN_PATH("No column part of a join path. Join columns: [%s]") {
      Object[] getFormatPlaceholders(Set<CandidateTablePruneCause> causes) {
        List<String> columns = new ArrayList<String>();
        for (CandidateTablePruneCause cause : causes) {
          columns.addAll(cause.getJoinColumns());
        }
        return new String[]{columns.toString()};
      }
    },
    // cube table is an aggregated fact and queried column is not under default
    // aggregate
    MISSING_DEFAULT_AGGREGATE("Columns: [%s] are missing default aggregate") {
      Object[] getFormatPlaceholders(Set<CandidateTablePruneCause> causes) {
        List<String> columns = new ArrayList<String>();
        for (CandidateTablePruneCause cause : causes) {
          columns.addAll(cause.getColumnsMissingDefaultAggregate());
        }
        return new String[]{columns.toString()};
      }
    },
    // missing partitions for cube table
    MISSING_PARTITIONS("Missing partitions for the cube table: %s") {
      Object[] getFormatPlaceholders(Set<CandidateTablePruneCause> causes) {
        Set<Set<String>> missingPartitions = Sets.newHashSet();
        for (CandidateTablePruneCause cause : causes) {
          missingPartitions.add(cause.getMissingPartitions());
        }
        return new String[]{missingPartitions.toString()};
      }
    };


    String errorFormat;

    CandidateTablePruneCode(String format) {
      this.errorFormat = format;
    }

    Object[] getFormatPlaceholders(Set<CandidateTablePruneCause> causes) {
      return null;
    }

    String getBriefError(Set<CandidateTablePruneCause> causes) {
      try {
        return String.format(errorFormat, getFormatPlaceholders(causes));
      } catch (NullPointerException e) {
        return name();
      }
    }
  }

  public enum SkipStorageCode {
    // invalid storage table
    INVALID,
    // storage table does not exist
    TABLE_NOT_EXIST,
    // storage has no update periods queried
    MISSING_UPDATE_PERIODS,
    // no candidate update periods, update period cause will have why each
    // update period is not a candidate
    NO_CANDIDATE_PERIODS,
    // storage table has no partitions queried
    NO_PARTITIONS,
    // partition column does not exist
    PART_COL_DOES_NOT_EXIST,
    // Range is not supported by this storage table
    RANGE_NOT_ANSWERABLE,
    // storage is not supported by execution engine
    UNSUPPORTED
  }

  public enum SkipUpdatePeriodCode {
    // invalid update period
    INVALID,
    // Query max interval is more than update period
    QUERY_INTERVAL_BIGGER
  }

  @JsonWriteNullProperties(false)
  @Data
  @NoArgsConstructor
  public static class SkipStorageCause {
    private SkipStorageCode cause;
    // update period to skip cause
    private Map<String, SkipUpdatePeriodCode> updatePeriodRejectionCause;
    private List<String> nonExistantPartCols;

    public SkipStorageCause(SkipStorageCode cause) {
      this.cause = cause;
    }

    public static SkipStorageCause partColDoesNotExist(String... partCols) {
      SkipStorageCause ret = new SkipStorageCause(SkipStorageCode.PART_COL_DOES_NOT_EXIST);
      ret.nonExistantPartCols = new ArrayList<String>();
      for (String s : partCols) {
        ret.nonExistantPartCols.add(s);
      }
      return ret;
    }

    public static SkipStorageCause noCandidateUpdatePeriod(Map<String, SkipUpdatePeriodCode> causes) {
      SkipStorageCause ret = new SkipStorageCause(SkipStorageCode.NO_CANDIDATE_PERIODS);
      ret.updatePeriodRejectionCause = causes;
      return ret;
    }
  }

  // cause for cube table
  private CandidateTablePruneCode cause;
  // storage to skip storage cause
  private Map<String, SkipStorageCause> storageCauses;

  // populated only incase of missing partitions cause
  private Set<String> missingPartitions;
  // populated only incase of missing update periods cause
  private List<String> missingUpdatePeriods;
  // populated in case of missing columns
  private List<String> missingColumns;
  // populated in case of expressions not evaluable
  private List<String> missingExpressions;
  // populated in case of no column part of a join path
  private List<String> joinColumns;
  // the columns that are missing default aggregate. only set in case of MISSING_DEFAULT_AGGREGATE
  private List<String> columnsMissingDefaultAggregate;
  // if a time dim is not supported by the fact. Would be set if and only if
  // the fact is not partitioned by part col of the time dim and time dim is not a dim attribute
  private Set<String> unsupportedTimeDims;
  // time covered
  private MaxCoveringFactResolver.TimeCovered maxTimeCovered;
  // ranges in which fact is invalid
  private List<TimeRange> invalidRanges;

  public CandidateTablePruneCause(CandidateTablePruneCode cause) {
    this.cause = cause;
  }

  // Different static constructors for different causes.
  public static CandidateTablePruneCause factNotAvailableInRange(List<TimeRange> ranges) {
    CandidateTablePruneCause cause = new CandidateTablePruneCause(FACT_NOT_AVAILABLE_IN_RANGE);
    cause.invalidRanges = ranges;
    return cause;
  }
  public static CandidateTablePruneCause timeDimNotSupported(Set<String> unsupportedTimeDims) {
    CandidateTablePruneCause cause = new CandidateTablePruneCause(TIMEDIM_NOT_SUPPORTED);
    cause.unsupportedTimeDims = unsupportedTimeDims;
    return cause;
  }

  public static CandidateTablePruneCause columnNotFound(Collection<String>... missingColumns) {
    List<String> colList = new ArrayList<String>();
    for (Collection<String> missing : missingColumns) {
      colList.addAll(missing);
    }
    CandidateTablePruneCause cause = new CandidateTablePruneCause(COLUMN_NOT_FOUND);
    cause.setMissingColumns(colList);
    return cause;
  }

  public static CandidateTablePruneCause columnNotFound(String... columns) {
    List<String> colList = new ArrayList<String>();
    for (String column : columns) {
      colList.add(column);
    }
    return columnNotFound(colList);
  }

  public static CandidateTablePruneCause expressionNotEvaluable(String... exprs) {
    List<String> colList = new ArrayList<String>();
    for (String column : exprs) {
      colList.add(column);
    }
    CandidateTablePruneCause cause = new CandidateTablePruneCause(EXPRESSION_NOT_EVALUABLE);
    cause.setMissingExpressions(colList);
    return cause;
  }

  public static CandidateTablePruneCause missingPartitions(Set<String> nonExistingParts) {
    CandidateTablePruneCause cause =
      new CandidateTablePruneCause(MISSING_PARTITIONS);
    cause.setMissingPartitions(nonExistingParts);
    return cause;
  }

  public static CandidateTablePruneCause lessData(MaxCoveringFactResolver.TimeCovered timeCovered) {
    CandidateTablePruneCause cause = new CandidateTablePruneCause(LESS_DATA);
    cause.setMaxTimeCovered(timeCovered);
    return cause;
  }

  public static CandidateTablePruneCause noColumnPartOfAJoinPath(final Collection<String> colSet) {
    CandidateTablePruneCause cause =
      new CandidateTablePruneCause(NO_COLUMN_PART_OF_A_JOIN_PATH);
    cause.setJoinColumns(new ArrayList<String>() {
      {
        addAll(colSet);
      }
    });
    return cause;
  }

  public static CandidateTablePruneCause noCandidateStorages(Map<String, SkipStorageCause> storageCauses) {
    CandidateTablePruneCause cause = new CandidateTablePruneCause(NO_CANDIDATE_STORAGES);
    cause.setStorageCauses(new HashMap<String, SkipStorageCause>());
    for (Map.Entry<String, SkipStorageCause> entry : storageCauses.entrySet()) {
      String key = entry.getKey();
      key = key.substring(0, (key.indexOf("_") + key.length() + 1) % (key.length() + 1)); // extract the storage part
      cause.getStorageCauses().put(key.toLowerCase(), entry.getValue());
    }
    return cause;
  }

  public static CandidateTablePruneCause missingDefaultAggregate(String... names) {
    CandidateTablePruneCause cause = new CandidateTablePruneCause(MISSING_DEFAULT_AGGREGATE);
    cause.setColumnsMissingDefaultAggregate(Lists.newArrayList(names));
    return cause;
  }
}
