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

import org.apache.commons.lang.StringUtils;
import org.apache.lens.cube.metadata.AbstractCubeTable;

import org.codehaus.jackson.annotate.JsonWriteNullProperties;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

public class PruneCauses<T extends AbstractCubeTable> extends HashMap<T, List<CandidateTablePruneCause>> {
  @Getter(lazy=true) private final HashMap<CandidateTablePruneCause, List<T>> reversed = reverse();
  @Getter(lazy=true) private final BriefAndDetailedError jsonObject = toJsonObject();
  public void addPruningMsg(T table, CandidateTablePruneCause msg) {
    if (get(table) == null) {
      put(table, new ArrayList<CandidateTablePruneCause>());
    }
    get(table).add(msg);
  }

  public HashMap<CandidateTablePruneCause, List<T>> reverse() {
    HashMap<CandidateTablePruneCause, List<T>> result = new HashMap<CandidateTablePruneCause, List<T>>();
    for(T key: keySet()) {
      for(CandidateTablePruneCause value: get(key)) {
        if(result.get(value) == null) {
          result.put(value, new ArrayList<T>());
        }
        result.get(value).add(key);
      }
    }
    return result;
  }

  public BriefAndDetailedError toJsonObject() {
    final HashMap<String, CandidateTablePruneCause> detailedMessage= new HashMap<String, CandidateTablePruneCause>();
    for(Map.Entry<CandidateTablePruneCause, List<T>> entry: getReversed().entrySet()) {
      detailedMessage.put(StringUtils.join(entry.getValue(), ","), entry.getKey());
    }
    return new BriefAndDetailedError(getBriefCause(), detailedMessage);
  }

  public String getBriefCause() {
    CandidateTablePruneCause.CubeTableCause maxCause = CandidateTablePruneCause.CubeTableCause.values()[0];
    for(CandidateTablePruneCause cause: getReversed().keySet()) {
      if(cause.getCause().compareTo(maxCause) > 0) {
        maxCause = cause.getCause();
      }
    }
    Map<CandidateTablePruneCause, List<T>> maxCauseMap = new HashMap<CandidateTablePruneCause, List<T>>();
    for(Map.Entry<CandidateTablePruneCause, List<T>> entry: getReversed().entrySet()) {
      if(entry.getKey().getCause().compareTo(maxCause) == 0) {
        maxCauseMap.put(entry.getKey(), entry.getValue());
      }
    }
    return maxCause.getBriefError(maxCauseMap.keySet());
  }

  public static void main(String[] args) {
    new BriefAndDetailedError();
  }

  @JsonWriteNullProperties(false)
  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static final class BriefAndDetailedError {
    public String brief;
    public HashMap<String, CandidateTablePruneCause> details;
  }
}
