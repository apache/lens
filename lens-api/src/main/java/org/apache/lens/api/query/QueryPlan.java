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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * The Class QueryPlan.
 */
@XmlRootElement
/**
 * Instantiates a new query plan.
 *
 * @param numJoins
 *          the num joins
 * @param numGbys
 *          the num gbys
 * @param numSels
 *          the num sels
 * @param numSelDi
 *          the num sel di
 * @param numHaving
 *          the num having
 * @param numObys
 *          the num obys
 * @param numAggrExprs
 *          the num aggr exprs
 * @param numFilters
 *          the num filters
 * @param tablesQueried
 *          the tables queried
 * @param hasSubQuery
 *          the has sub query
 * @param execMode
 *          the exec mode
 * @param scanMode
 *          the scan mode
 * @param tableWeights
 *          the table weights
 * @param joinWeight
 *          the join weight
 * @param gbyWeight
 *          the gby weight
 * @param filterWeight
 *          the filter weight
 * @param havingWeight
 *          the having weight
 * @param obyWeight
 *          the oby weight
 * @param selectWeight
 *          the select weight
 * @param prepareHandle
 *          the prepare handle
 * @param planString
 *          the plan string
 * @param queryCost
 *          the query cost
 * @param error
 *          the error
 * @param errorMsg
 *          the error msg
 */
@AllArgsConstructor
/**
 * Instantiates a new query plan.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class QueryPlan extends QuerySubmitResult {

  /** The num joins. */
  @XmlElement
  @Getter
  private int numJoins = 0;

  /** The num gbys. */
  @XmlElement
  @Getter
  private int numGbys = 0;

  /** The num sels. */
  @XmlElement
  @Getter
  private int numSels = 0;

  /** The num sel di. */
  @XmlElement
  @Getter
  private int numSelDi = 0;

  /** The num having. */
  @XmlElement
  @Getter
  private int numHaving = 0;

  /** The num obys. */
  @XmlElement
  @Getter
  private int numObys = 0;

  /** The num aggr exprs. */
  @XmlElement
  @Getter
  private int numAggrExprs = 0;

  /** The num filters. */
  @XmlElement
  @Getter
  private int numFilters = 0;

  /** The tables queried. */
  @XmlElementWrapper
  @Getter
  private List<String> tablesQueried;

  /** The has sub query. */
  @XmlElement
  @Getter
  private boolean hasSubQuery = false;

  /** The exec mode. */
  @XmlElement
  @Getter
  private String execMode;

  /** The scan mode. */
  @XmlElement
  @Getter
  private String scanMode;

  /** The table weights. */
  @XmlElementWrapper
  @Getter
  private Map<String, Double> tableWeights;

  /** The join weight. */
  @XmlElement
  @Getter
  private Double joinWeight;

  /** The gby weight. */
  @XmlElement
  @Getter
  private Double gbyWeight;

  /** The filter weight. */
  @XmlElement
  @Getter
  private Double filterWeight;

  /** The having weight. */
  @XmlElement
  @Getter
  private Double havingWeight;

  /** The oby weight. */
  @XmlElement
  @Getter
  private Double obyWeight;

  /** The select weight. */
  @XmlElement
  @Getter
  private Double selectWeight;

  /** The prepare handle. */
  @Getter
  @Setter
  private QueryPrepareHandle prepareHandle;

  /** The plan string. */
  @XmlElement
  private String planString;

  /** The query cost. */
  @XmlElement
  @Getter
  private QueryCost queryCost;

  /** The error. */
  @XmlElement
  @Getter
  private boolean error = false;

  /** The error msg. */
  @XmlElement
  @Getter
  private String errorMsg;

  public String getPlanString() throws UnsupportedEncodingException {
    return URLDecoder.decode(planString, "UTF-8");
  }

  /**
   * Instantiates a new query plan.
   *
   * @param hasError
   *          the has error
   * @param errorMsg
   *          the error msg
   */
  public QueryPlan(boolean hasError, String errorMsg) {
    this.error = hasError;
    this.errorMsg = errorMsg;
  }
}
