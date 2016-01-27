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

import org.apache.lens.cube.metadata.CubeDimensionTable;
import org.apache.lens.cube.metadata.Dimension;
import org.apache.lens.cube.metadata.StorageConstants;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.session.SessionState;

import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.Setter;

/**
 * Holds context of a candidate dim table.
 */
public class CandidateDim implements CandidateTable {
  final CubeDimensionTable dimtable;
  @Getter
  @Setter
  private String storageTable;
  @Getter
  @Setter
  private String whereClause;
  private boolean dbResolved = false;
  private Map<String, Boolean> whereClauseAdded = new HashMap<>();
  private Dimension baseTable;

  public boolean isWhereClauseAdded() {
    return !whereClauseAdded.isEmpty();
  }

  public boolean isWhereClauseAdded(String alias) {
    return whereClauseAdded.get(alias) == null ? false : whereClauseAdded.get(alias);
  }

  public void setWhereClauseAdded(String alias) {
    this.whereClauseAdded.put(alias, true);
  }

  CandidateDim(CubeDimensionTable dimtable, Dimension dim) {
    this.dimtable = dimtable;
    this.baseTable = dim;
  }

  @Override
  public String toString() {
    return dimtable.toString();
  }

  public String getStorageString(String alias) {
    if (!dbResolved) {
      String database = SessionState.get().getCurrentDatabase();
      // Add database name prefix for non default database
      if (StringUtils.isNotBlank(database) && !"default".equalsIgnoreCase(database)) {
        storageTable = database + "." + storageTable;
      }
      dbResolved = true;
    }
    return storageTable + " " + alias;
  }

  @Override
  public Dimension getBaseTable() {
    return baseTable;
  }

  @Override
  public CubeDimensionTable getTable() {
    return dimtable;
  }

  @Override
  public String getName() {
    return dimtable.getName();
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    CandidateDim other = (CandidateDim) obj;

    if (this.getTable() == null) {
      if (other.getTable() != null) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((getTable() == null) ? 0 : getTable().getName().toLowerCase().hashCode());
    return result;
  }

  @Override
  public Collection<String> getColumns() {
    return Sets.union(dimtable.getAllFieldNames(), dimtable.getPartCols());
  }

  @Override
  public Set<String> getStorageTables() {
    return Collections.singleton(storageTable);
  }

  @Override
  public Set<String> getPartsQueried() {
    if (StringUtils.isBlank(whereClause)) {
      return Collections.emptySet();
    }
    return Collections.singleton(StorageConstants.LATEST_PARTITION_VALUE);
  }
}
