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

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString
@EqualsAndHashCode
public class FactPartitionBasedQueryCost implements QueryCost<FactPartitionBasedQueryCost>, Serializable {

  private final double partitionCost;

  public FactPartitionBasedQueryCost(final double partitionCost) {
    Preconditions.checkArgument(partitionCost >= 0, "Cost can't be negative");
    this.partitionCost = partitionCost;
  }

  @Override
  public FactPartitionBasedQueryCost add(final FactPartitionBasedQueryCost other) {
    return new FactPartitionBasedQueryCost(partitionCost + other.partitionCost);
  }

  @Override
  public QueryCostType getQueryCostType() {
    return partitionCost == 0 ? QueryCostType.LOW : QueryCostType.HIGH;
  }

  @Override
  public long getEstimatedExecTimeMillis() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Estimated time is not implemented");
  }

  @Override
  public double getEstimatedResourceUsage() {
    return partitionCost;
  }

  @Override
  public int compareTo(final FactPartitionBasedQueryCost o) {
    return new Double(partitionCost).compareTo(o.partitionCost);
  }

  @Override
  public String toString() {
    return getQueryCostType() + "(" + getEstimatedResourceUsage() + ")";
  }

  public static class Parser implements org.apache.lens.api.parse.Parser<FactPartitionBasedQueryCost> {

    @Override
    public FactPartitionBasedQueryCost parse(String value) {
      return new FactPartitionBasedQueryCost(Double.parseDouble(value));
    }
  }
}
