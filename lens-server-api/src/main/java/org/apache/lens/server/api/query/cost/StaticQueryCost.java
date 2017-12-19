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
package org.apache.lens.server.api.query.cost;

import java.io.Serializable;

import org.apache.lens.api.query.QueryCostType;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
@EqualsAndHashCode
public class StaticQueryCost implements QueryCost<StaticQueryCost>, Serializable {

  private final double staticCost;
  @Getter
  @Setter
  private QueryCostType queryCostType;

  public StaticQueryCost(final double cost) {
    this.staticCost = cost;
    this.queryCostType = QueryCostType.HIGH;
  }

  //Added for testcase
  public StaticQueryCost(final double cost, final QueryCostType queryCostType) {
    this.staticCost = cost;
    this.queryCostType = queryCostType;
  }

  @Override
  public StaticQueryCost add(final StaticQueryCost other) {
    return new StaticQueryCost(staticCost + other.staticCost);
  }

  @Override
  public long getEstimatedExecTimeMillis() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Estimated time is not implemented");
  }

  @Override
  public double getEstimatedResourceUsage() throws UnsupportedOperationException {
    return staticCost;
  }

  @Override
  public int compareTo(final StaticQueryCost staticQueryCost) {
    return new Double(staticCost).compareTo(staticQueryCost.staticCost);
  }

  @Override
  public String toString() {
    return getQueryCostType() + "(" + getEstimatedResourceUsage() + ")";
  }
}
