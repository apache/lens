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
/*
 *
 */
package org.apache.lens.api.query;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Result returned as call to query cost estimate
 */
@XmlRootElement
/**
 * Instantiates a new estimate result
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class EstimateResult extends QuerySubmitResult {


  /**
   * The query cost.
   */
  @XmlElement
  @Getter
  private QueryCost cost;

  /**
   * Set to true if estimate call resulted in a failure
   */
  @XmlElement
  @Getter
  private boolean error = false;

  /**
   * The error message in case of failure
   */
  @XmlElement
  @Getter
  private String errorMsg;

  /**
   * Instantiates a new CostEstimateResult with cost
   *
   * @param handle the handle
   */
  public EstimateResult(QueryCost cost) {
    this.cost = cost;
  }

  /**
   * Instantiates a new CostEstimateResult with error
   *
   * @param errorMessage
   */
  public EstimateResult(String errorMessage) {
    error = true;
    this.errorMsg = errorMessage;
  }
}
