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

import java.util.*;

import org.codehaus.jackson.annotate.JsonWriteNullProperties;

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
    MORE_WEIGHT("Picked table had more weight than minimum."),
    // cube table has more partitions
    MORE_PARTITIONS("Picked table has more partitions than minimum"),
    // invalid cube table
    INVALID("Invalid cube table provided in query"),
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
    // candidate table tries to get denormalized field from dimension and the
    // referred dimension is invalid.
    INVALID_DENORM_TABLE("Referred dimension is invalid in one of the candidate tables"),
    // missing storage tables for cube table
    MISSING_STORAGES("Missing storage tables for the cube table"),
    // no candidate storges for cube table, storage cause will have why each
    // storage is not a candidate
    NO_CANDIDATE_STORAGES("No candidate storages for any table"),
    // cube table has more weight
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
        List<List<String>> missingPartitions = new ArrayList<List<String>>();
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
  private List<String> missingPartitions;
  // populated only incase of missing update periods cause
  private List<String> missingUpdatePeriods;
  // populated in case of missing columns
  private List<String> missingColumns;
  // populated in case of no column part of a join path
  private List<String> joinColumns;
  // the columns that are missing default aggregate. only set in case of MISSING_DEFAULT_AGGREGATE
  private List<String> columnsMissingDefaultAggregate;

  public CandidateTablePruneCause(CandidateTablePruneCode cause) {
    this.cause = cause;
  }

  // Different static constructors for different causes.

  public static CandidateTablePruneCause columnNotFound(Collection<String> missingColumns) {
    List<String> colList = new ArrayList<String>();
    colList.addAll(missingColumns);
    CandidateTablePruneCause cause = new CandidateTablePruneCause(CandidateTablePruneCode.COLUMN_NOT_FOUND);
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

  public static CandidateTablePruneCause missingPartitions(List<String> nonExistingParts) {
    CandidateTablePruneCause cause =
      new CandidateTablePruneCause(CandidateTablePruneCode.MISSING_PARTITIONS);
    cause.setMissingPartitions(nonExistingParts);
    return cause;
  }

  public static CandidateTablePruneCause noColumnPartOfAJoinPath(final Collection<String> colSet) {
    CandidateTablePruneCause cause =
      new CandidateTablePruneCause(CandidateTablePruneCode.NO_COLUMN_PART_OF_A_JOIN_PATH);
    cause.setJoinColumns(new ArrayList<String>() {
      {
        addAll(colSet);
      }
    });
    return cause;
  }

  public static CandidateTablePruneCause noCandidateStorages(Map<String, SkipStorageCause> storageCauses) {
    CandidateTablePruneCause cause = new CandidateTablePruneCause(CandidateTablePruneCode.NO_CANDIDATE_STORAGES);
    cause.setStorageCauses(new HashMap<String, SkipStorageCause>());
    for (Map.Entry<String, SkipStorageCause> entry : storageCauses.entrySet()) {
      String key = entry.getKey();
      key = key.substring(0, (key.indexOf("_") + key.length() + 1) % (key.length() + 1)); // extract the storage part
      cause.getStorageCauses().put(key, entry.getValue());
    }
    return cause;
  }

  public static CandidateTablePruneCause missingDefaultAggregate(String... names) {
    CandidateTablePruneCause cause = new CandidateTablePruneCause(CandidateTablePruneCode.MISSING_DEFAULT_AGGREGATE);
    cause.setColumnsMissingDefaultAggregate(new ArrayList<String>());
    for (String name : names) {
      cause.getColumnsMissingDefaultAggregate().add(name);
    }
    return cause;
  }
}
