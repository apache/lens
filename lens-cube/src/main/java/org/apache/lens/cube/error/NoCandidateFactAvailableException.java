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
package org.apache.lens.cube.error;

import org.apache.lens.cube.parse.CubeQueryContext;
import org.apache.lens.cube.parse.PruneCauses;
import org.apache.lens.cube.parse.StorageCandidate;
import org.apache.lens.server.api.error.LensException;

import lombok.Getter;


/**
 * Note: This class is mainly meant for test cases to assert the detailed reasons (stored in
 * {@link #briefAndDetailedError} and {@link #cubeQueryContext}) leading to "No Candidate was found"
 */
public class NoCandidateFactAvailableException extends LensException {

  @Getter
  private final CubeQueryContext cubeQueryContext;
  @Getter
  private final PruneCauses<StorageCandidate> briefAndDetailedError;

  public NoCandidateFactAvailableException(CubeQueryContext cubeql) {
    this(cubeql.getStoragePruningMsgs().getBriefCause(), cubeql);
  }

  public NoCandidateFactAvailableException(String errMsg, CubeQueryContext cubeql) {
    super(LensCubeErrorCode.NO_CANDIDATE_FACT_AVAILABLE.getLensErrorInfo(), errMsg);
    this.cubeQueryContext = cubeql;
    this.briefAndDetailedError = cubeql.getStoragePruningMsgs();
  }


  public PruneCauses.BriefAndDetailedError getJsonMessage() {
    return briefAndDetailedError.toJsonObject();
  }

  @Override
  public int compareTo(LensException e) {
    //Compare the max CandidateTablePruneCode coming from different instances.
    if (e instanceof NoCandidateFactAvailableException) {
      return briefAndDetailedError.getMaxCause().compareTo(
               ((NoCandidateFactAvailableException) e).briefAndDetailedError.getMaxCause());
    }
    return super.compareTo(e);
  }
}
