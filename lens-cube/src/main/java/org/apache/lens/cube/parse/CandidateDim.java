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

import java.util.Collection;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.lens.cube.metadata.CubeDimensionTable;
import org.apache.lens.cube.metadata.Dimension;

/**
 * Holds context of a candidate dim table.
 * 
 */
class CandidateDim implements CandidateTable {
  final CubeDimensionTable dimtable;
  String storageTable;
  String whereClause;
  private boolean dbResolved = false;
  private boolean whereClauseAdded = false;
  private Dimension baseTable;

  public boolean isWhereClauseAdded() {
    return whereClauseAdded;
  }

  public void setWhereClauseAdded() {
    this.whereClauseAdded = true;
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
    return dimtable.getAllFieldNames();
  }
}
