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
package org.apache.lens.api.result;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.lens.api.query.QueryCostType;
import org.apache.lens.api.query.QuerySubmitResult;

import lombok.*;

/**
 * The Class QueryCostTO.
 */
@XmlRootElement
/**
 * Instantiates a new query cost.
 *
 * @param estimatedExecTimeMillis
 *          the estimated exec time millis
 * @param estimatedResourceUsage
 *          the estimated resource usage
 */
@AllArgsConstructor
/**
 * Instantiates a new query cost.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class QueryCostTO extends QuerySubmitResult implements Serializable {

  /**
   * The estimated exec time millis.
   */
  @XmlElement
  @Getter
  private Long estimatedExecTimeMillis;

  /**
   * The estimated resource usage.
   */
  @XmlElement
  @Getter
  private Double estimatedResourceUsage;

  /**
   * Normalized query cost, calculated by driver
   */
  @XmlElement
  @Getter
  private QueryCostType costType;

}
