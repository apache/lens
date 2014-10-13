package org.apache.lens.api.query;

/*
 * #%L
 * Grill API
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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

@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class QueryPlan extends QuerySubmitResult {
  @XmlElement @Getter private int numJoins = 0;
  @XmlElement @Getter private int numGbys = 0;
  @XmlElement @Getter private int numSels = 0;
  @XmlElement @Getter private int numSelDi = 0;
  @XmlElement @Getter private int numHaving = 0;
  @XmlElement @Getter private int numObys = 0;
  @XmlElement @Getter private int numAggrExprs = 0;
  @XmlElement @Getter private int numFilters = 0;
  @XmlElementWrapper @Getter private List<String> tablesQueried;
  @XmlElement @Getter private boolean hasSubQuery = false;
  @XmlElement @Getter private String execMode;
  @XmlElement @Getter private String scanMode;
  @XmlElementWrapper @Getter private Map<String, Double> tableWeights;
  @XmlElement @Getter private Double joinWeight;
  @XmlElement @Getter private Double gbyWeight;
  @XmlElement @Getter private Double filterWeight;
  @XmlElement @Getter private Double havingWeight;
  @XmlElement @Getter private Double obyWeight;
  @XmlElement @Getter private Double selectWeight;
  @Getter @Setter private QueryPrepareHandle prepareHandle;
  @XmlElement private String planString;
  @XmlElement @Getter private QueryCost queryCost;
  @XmlElement @Getter private boolean hasError = false;
  @XmlElement @Getter private String errorMsg;
  
  public String getPlanString() throws UnsupportedEncodingException {
    return URLDecoder.decode(planString, "UTF-8");
  }

  public QueryPlan(boolean hasError, String errorMsg) {
    this.hasError = hasError;
    this.errorMsg = errorMsg;
  }
}
