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

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

import java.util.*;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.*;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.plan.PlanUtils;

import lombok.extern.slf4j.Slf4j;

/**
 * Finds all timeranges in the query and does validation wrt the queried field's life and the range queried
 */
@Slf4j
class TimerangeResolver implements ContextRewriter {
  public TimerangeResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
    if (cubeql.getCube() == null) {
      return;
    }
    extractTimeRange(cubeql);
  }


  private void extractTimeRange(CubeQueryContext cubeql) throws LensException {
    // get time range -
    // Time range should be direct child of where condition
    // TOK_WHERE.TOK_FUNCTION.Identifier Or, it should be right hand child of
    // AND condition TOK_WHERE.KW_AND.TOK_FUNCTION.Identifier
    if (cubeql.getWhereAST() == null || cubeql.getWhereAST().getChildCount() < 1) {
      throw new LensException(LensCubeErrorCode.NO_TIMERANGE_FILTER.getLensErrorInfo());
    }
    searchTimeRanges(cubeql.getWhereAST(), cubeql, null, 0);
  }

  private void searchTimeRanges(ASTNode root, CubeQueryContext cubeql, ASTNode parent, int childIndex)
    throws LensException {
    if (root == null) {
      return;
    } else if (root.getToken().getType() == TOK_FUNCTION) {
      ASTNode fname = HQLParser.findNodeByPath(root, Identifier);
      if (fname != null && CubeQueryContext.TIME_RANGE_FUNC.equalsIgnoreCase(fname.getText())) {
        processTimeRangeFunction(cubeql, root, parent, childIndex);
      }
    } else {
      for (int i = 0; i < root.getChildCount(); i++) {
        ASTNode child = (ASTNode) root.getChild(i);
        searchTimeRanges(child, cubeql, root, i);
      }
    }
  }

  private String getColumnName(ASTNode node) {
    String column = null;
    if (node.getToken().getType() == DOT) {
      ASTNode colIdent = (ASTNode) node.getChild(1);
      column = colIdent.getText().toLowerCase();
    } else if (node.getToken().getType() == TOK_TABLE_OR_COL) {
      // Take child ident.totext
      ASTNode ident = (ASTNode) node.getChild(0);
      column = ident.getText().toLowerCase();
    }
    return column;
  }

  private void processTimeRangeFunction(CubeQueryContext cubeql, ASTNode timenode, ASTNode parent, int childIndex)
    throws LensException {
    TimeRange.TimeRangeBuilder builder = TimeRange.getBuilder();
    builder.astNode(timenode);
    builder.parent(parent);
    builder.childIndex(childIndex);

    String timeDimName = getColumnName((ASTNode) timenode.getChild(1));

    if (!cubeql.getCube().getTimedDimensions().contains(timeDimName)) {
      throw new LensException(LensCubeErrorCode.NOT_A_TIMED_DIMENSION.getLensErrorInfo(), timeDimName);
    }
    // Replace timeDimName with column which is used for partitioning. Assume
    // the same column
    // is used as a partition column in all storages of the fact
    timeDimName = cubeql.getPartitionColumnOfTimeDim(timeDimName);
    builder.partitionColumn(timeDimName);

    String fromDateRaw = PlanUtils.stripQuotes(timenode.getChild(2).getText());
    String toDateRaw = null;
    if (timenode.getChildCount() > 3) {
      ASTNode toDateNode = (ASTNode) timenode.getChild(3);
      if (toDateNode != null) {
        toDateRaw = PlanUtils.stripQuotes(timenode.getChild(3).getText());
      }
    }
    long currentTime = cubeql.getConf().getLong(LensConfConstants.QUERY_CURRENT_TIME_IN_MILLIS, 0);
    Date now;
    if (currentTime != 0) {
      now = new Date(currentTime);
    } else {
      now = new Date();
    }
    builder.fromDate(DateUtil.resolveDate(fromDateRaw, now));
    if (StringUtils.isNotBlank(toDateRaw)) {
      builder.toDate(DateUtil.resolveDate(toDateRaw, now));
    } else {
      builder.toDate(now);
    }

    TimeRange range = builder.build();
    range.validate();
    cubeql.getTimeRanges().add(range);
  }


}
