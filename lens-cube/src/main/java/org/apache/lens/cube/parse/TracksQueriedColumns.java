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

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import lombok.Getter;

abstract class TracksQueriedColumns implements TrackQueriedColumns {

  @Getter
  private Map<String, Set<String>> tblAliasToColumns = new HashMap<>();

  public void addColumnsQueried(String tblAlias, String column) {

    Set<String> cols = tblAliasToColumns.get(tblAlias.toLowerCase());
    if (cols == null) {
      cols = new LinkedHashSet<>();
      tblAliasToColumns.put(tblAlias.toLowerCase(), cols);
    }
    cols.add(column);
  }

  public void addColumnsQueried(Map<String, Set<String>> tblAliasToColumns) {

    for (Map.Entry<String, Set<String>> entry : tblAliasToColumns.entrySet()) {
      Set<String> cols = this.tblAliasToColumns.get(entry.getKey());
      if (cols == null) {
        cols = new LinkedHashSet<>();
        this.tblAliasToColumns.put(entry.getKey(), cols);
      }
      cols.addAll(entry.getValue());
    }
  }

  public Set<String> getColumnsQueried(String tblAlias) {
    return tblAliasToColumns.get(tblAlias);
  }

}
