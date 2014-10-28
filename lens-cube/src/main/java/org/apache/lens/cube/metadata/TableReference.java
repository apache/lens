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

public class TableReference {
  private String destTable;
  private String destColumn;

  public TableReference() {
  }

  public TableReference(String destTable, String destColumn) {
    this.destTable = destTable;
    this.destColumn = destColumn;
  }

  public TableReference(String reference) {
    String desttoks[] = reference.split("\\.+");
    this.destTable = desttoks[0];
    this.destColumn = desttoks[1];
  }

  public String getDestTable() {
    return destTable;
  }

  public void setDestTable(String dest) {
    this.destTable = dest;
  }

  public String getDestColumn() {
    return destColumn;
  }

  public void setDestColumn(String destColumn) {
    this.destColumn = destColumn;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    TableReference other = (TableReference) obj;
    if (this.getDestColumn() == null) {
      if (other.getDestColumn() != null) {
        return false;
      }
    } else if (!this.getDestColumn().equals(other.getDestColumn())) {
      return false;
    }
    if (this.getDestTable() == null) {
      if (other.getDestTable() != null) {
        return false;
      }
    } else if (!this.getDestTable().equals(other.getDestTable())) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return destTable + "." + destColumn;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((destColumn == null) ? 0 : destColumn.hashCode());
    result = prime * result + ((destTable == null) ? 0 : destTable.hashCode());
    return result;
  }
}
