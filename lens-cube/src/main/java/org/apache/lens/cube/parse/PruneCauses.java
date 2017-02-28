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

import static com.google.common.collect.Sets.newHashSet;
import static java.util.stream.Collectors.toMap;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode;

import org.apache.commons.lang.StringUtils;

import org.codehaus.jackson.annotate.JsonWriteNullProperties;

import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

public class PruneCauses<T> extends HashMap<T, List<CandidateTablePruneCause>> {
  @Getter(lazy = true)
  private final HashMap<CandidateTablePruneCause, List<T>> reversed = reverse();
  @Getter(lazy = true)
  private final HashMap<String, List<CandidateTablePruneCause>> compact = computeCompact();
  @Getter(lazy = true)
  private final CandidateTablePruneCode maxCause  = computeMaxCause();

  private HashMap<String, List<CandidateTablePruneCause>> computeCompact() {
    HashMap<String, List<CandidateTablePruneCause>> detailedMessage = Maps.newHashMap();
    for (Map.Entry<CandidateTablePruneCause, List<T>> entry : getReversed().entrySet()) {
      String key = StringUtils.join(entry.getValue(), ",");
      detailedMessage.computeIfAbsent(key, k -> new ArrayList<>()).add(entry.getKey());
    }
    return detailedMessage;
  }

  @Getter(lazy = true)
  private final BriefAndDetailedError jsonObject = toJsonObject();

  public void addPruningMsg(T table, CandidateTablePruneCause msg) {
    if (get(table) == null) {
      put(table, new ArrayList<CandidateTablePruneCause>());
    }
    get(table).add(msg);
  }

  private HashMap<CandidateTablePruneCause, List<T>> reverse() {
    HashMap<CandidateTablePruneCause, List<T>> result = new HashMap<CandidateTablePruneCause, List<T>>();
    for (T key : keySet()) {
      for (CandidateTablePruneCause value : get(key)) {
        result.computeIfAbsent(value, k -> new ArrayList<>()).add(key);
      }
    }
    return result;
  }

  public BriefAndDetailedError toJsonObject() {
    return new BriefAndDetailedError(getBriefCause(), getCompact());
  }

  private CandidateTablePruneCode computeMaxCause() {
    CandidateTablePruneCode maxCause = CandidateTablePruneCode.values()[0];
    for (CandidateTablePruneCause cause : getReversed().keySet()) {
      if (cause.getCause().compareTo(maxCause) > 0) {
        maxCause = cause.getCause();
      }
    }
    return maxCause;
  }

  public String getBriefCause() {
    CandidateTablePruneCode maxCause = getReversed().keySet().stream()
      .map(CandidateTablePruneCause::getCause).max(Comparator.naturalOrder()).get();
    Map<CandidateTablePruneCause, String> maxCauseMap = Maps.newHashMap();
    for (Map.Entry<CandidateTablePruneCause, List<T>> entry : getReversed().entrySet()) {
      if (entry.getKey().getCause().equals(maxCause)) {
        maxCauseMap.put(entry.getKey(), StringUtils.join(entry.getValue(), ","));
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
    private String brief;
    private HashMap<String, List<CandidateTablePruneCause>> details;

    Map<HashSet<String>, List<CandidateTablePruneCause>> enhanced() {
      return getDetails().entrySet().stream().collect(toMap(
        o -> newHashSet(o.getKey().split(",")),
        Map.Entry::getValue));
    }
  }
}
