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

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.lens.api.result.QueryCostTO;

import lombok.*;

/**
 * The Class QueryPlan.
 */
@XmlRootElement
/**
 * Instantiates a new query plan.
 *
 * @param tablesQueried
 *          the tables queried
 * @param hasSubQuery
 *          the has sub query
 * @param execMode
 *          the exec mode
 * @param scanMode
 *          the scan mode
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

  /**
   * The tables queried.
   */
  @XmlElementWrapper
  @Getter
  private List<String> tablesQueried;

  /**
   * The has sub query.
   */
  @XmlElement
  @Getter
  private boolean hasSubQuery = false;

  /**
   * The exec mode.
   */
  @XmlElement
  @Getter
  private String execMode;

  /**
   * The scan mode.
   */
  @XmlElement
  @Getter
  private String scanMode;

  /**
   * The prepare handle.
   */
  @Getter
  @Setter
  private QueryPrepareHandle prepareHandle;

  /**
   * The plan string.
   */
  @XmlElement
  private String planString;

  /**
   * The query cost.
   */
  @XmlElement
  @Getter
  private QueryCostTO queryCost;

  /**
   * The error.
   */
  @XmlElement
  @Getter
  private boolean error = false;

  /**
   * The error msg.
   */
  @XmlElement
  @Getter
  private String errorMsg;

  public String getPlanString() throws UnsupportedEncodingException {
    return URLDecoder.decode(planString, "UTF-8");
  }

  /**
   * Instantiates a new query plan.
   *
   * @param hasError the has error
   * @param errorMsg the error msg
   */
  public QueryPlan(boolean hasError, String errorMsg) {
    this.error = hasError;
    this.errorMsg = errorMsg;
  }
}
