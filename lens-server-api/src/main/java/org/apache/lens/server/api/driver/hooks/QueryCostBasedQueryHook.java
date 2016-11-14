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
package org.apache.lens.server.api.driver.hooks;

import static org.apache.lens.server.api.LensConfConstants.DEFAULT_QUERY_COST_PARSER;
import static org.apache.lens.server.api.LensConfConstants.QUERY_COST_PARSER;

import org.apache.lens.api.parse.Parser;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.cost.QueryCost;

import com.google.common.collect.*;

/**
 * This hook allows for a driver to discard queries based on the cost of the query. This works on two
 * configurations values, among which either, both or none can be set. The configuration values are picked from
 * driver configuration. The driver config can set `allowed.range.sets` as a mathematical range of costs that
 * are allowed on this driver. Queries having cost among these will be allowed
 * and the rest will be discarded. On the other hand, the driver can set `disallowed.range.sets` as a
 * mathematical range of costs that are disallowed on this driver. Queries having range among these
 * will be rejected, and the rest will be allowed.
 *
 * Mathematical range looks like the following:
 *
 * (10, 20] U (30, 40) U [1000, )
 *
 *
 */
public class QueryCostBasedQueryHook<T extends QueryCost<T>> extends NoOpDriverQueryHook {
  public static final String ALLOWED_RANGE_SETS = "allowed.range.sets";
  public static final String DISALLOWED_RANGE_SETS = "disallowed.range.sets";
  private RangeSet<T> allowedCosts;
  private Parser<T> parser;

  public RangeSet<T> parseAllowedRangeSet(String rangeStr) {
    RangeSet<T> parsed = parseRangeSet(rangeStr);
    if (parsed == null) {
      TreeRangeSet<T> set = TreeRangeSet.create();
      set.add(Range.<T>all());
      return set;
    } else {
      return parsed;
    }
  }

  public RangeSet<T> parseDisallowedRangeSet(String rangeStr) {
    RangeSet<T> parsed = parseRangeSet(rangeStr);
    if (parsed == null) {
      return TreeRangeSet.create();
    } else {
      return parsed;
    }
  }

  public RangeSet<T> parseRangeSet(String rangeStr) {
    if (rangeStr == null) {
      return null;
    }
    RangeSet<T> set = TreeRangeSet.create();
    for (String range : rangeStr.split("U")) {
      set.add(parseRange(range));
    }
    return set;
  }

  public Range<T> parseRange(String rangeStr) {
    if (rangeStr == null) {
      return null;
    }
    rangeStr = rangeStr.trim();
    BoundType lowerBound, upperBound;
    if (rangeStr.startsWith("[")) {
      lowerBound = BoundType.CLOSED;
    } else if (rangeStr.startsWith("(")) {
      lowerBound = BoundType.OPEN;
    } else {
      throw new IllegalArgumentException("Range should start with either ( or [");
    }
    if (rangeStr.endsWith("]")) {
      upperBound = BoundType.CLOSED;
    } else if (rangeStr.endsWith(")")) {
      upperBound = BoundType.OPEN;
    } else {
      throw new IllegalArgumentException("Range should end with either ) or ]");
    }
    String[] pair = rangeStr.substring(1, rangeStr.length() - 1).split(",");
    String leftStr = pair[0].trim();
    String rightStr = pair[1].trim();
    if (leftStr.isEmpty() && rightStr.isEmpty()) {
      return Range.all();
    } else if (leftStr.isEmpty()) {
      return Range.upTo(parser.parse(rightStr), upperBound);
    } else if (rightStr.isEmpty()) {
      return Range.downTo(parser.parse(leftStr), lowerBound);
    } else {
      return Range.range(parser.parse(leftStr), lowerBound, parser.parse(rightStr), upperBound);
    }
  }

  @Override
  public void setDriver(LensDriver driver) {
    super.setDriver(driver);
    Class<? extends Parser<T>> parserClass = (Class<? extends Parser<T>>) driver.getConf().getClass(QUERY_COST_PARSER,
      DEFAULT_QUERY_COST_PARSER, Parser.class);
    try {
      this.parser = parserClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new IllegalArgumentException("Couldn't initialize query cost parser from class " + parserClass);
    }
    allowedCosts = parseAllowedRangeSet(driver.getConf().get(ALLOWED_RANGE_SETS));
    allowedCosts.removeAll(parseDisallowedRangeSet(driver.getConf().get(DISALLOWED_RANGE_SETS)));
  }

  @Override
  public void postEstimate(AbstractQueryContext ctx) throws LensException {
    if (!allowedCosts.contains((T) ctx.getDriverQueryCost(getDriver()))) {
      throw new LensException("Driver " + getDriver() + " doesn't accept query "
        + "with cost " + ctx.getSelectedDriverQueryCost() + ". Allowed ranges: " + allowedCosts);
    }
  }
}
