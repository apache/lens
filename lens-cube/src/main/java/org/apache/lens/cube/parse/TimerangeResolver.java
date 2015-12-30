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

import static org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode.FACT_NOT_AVAILABLE_IN_RANGE;

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

import java.util.*;

import org.apache.lens.cube.error.ColUnAvailableInTimeRange;
import org.apache.lens.cube.error.ColUnAvailableInTimeRangeException;
import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.*;
import org.apache.lens.cube.parse.DenormalizationResolver.ReferencedQueriedColumn;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.plan.PlanUtils;

import com.google.common.collect.Lists;
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
    doColLifeValidation(cubeql);
    doFactRangeValidation(cubeql);
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

    Date now = new Date();
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

  private void doColLifeValidation(CubeQueryContext cubeql) throws LensException,
    ColUnAvailableInTimeRangeException {
    Set<String> cubeColumns = cubeql.getColumnsQueried(cubeql.getCube().getName());
    if (cubeColumns == null || cubeColumns.isEmpty()) {
      // Query doesn't have any columns from cube
      return;
    }

    for (String col : cubeql.getColumnsQueried(cubeql.getCube().getName())) {
      CubeColumn column = cubeql.getCube().getColumnByName(col);
      for (TimeRange range : cubeql.getTimeRanges()) {
        if (column == null) {
          if (!cubeql.getCube().getTimedDimensions().contains(col)) {
            throw new LensException(LensCubeErrorCode.NOT_A_CUBE_COLUMN.getLensErrorInfo(), col);
          }
          continue;
        }
        if (!column.isColumnAvailableInTimeRange(range)) {
          throwException(column);
        }
      }
    }

    // Look at referenced columns through denormalization resolver
    // and do column life validation
    Map<String, Set<ReferencedQueriedColumn>> refCols = cubeql.getDeNormCtx().getReferencedCols();
    for (String col : refCols.keySet()) {
      Iterator<ReferencedQueriedColumn> refColIter = refCols.get(col).iterator();
      while (refColIter.hasNext()) {
        ReferencedQueriedColumn refCol = refColIter.next();
        for (TimeRange range : cubeql.getTimeRanges()) {
          if (!refCol.col.isColumnAvailableInTimeRange(range)) {
            log.debug("The refernced column: {} is not in the range queried", refCol.col.getName());
            refColIter.remove();
            break;
          }
        }
      }
    }

    // Remove join paths that have columns with invalid life span
    AutoJoinContext joinContext = cubeql.getAutoJoinCtx();
    if (joinContext == null) {
      return;
    }
    // Get cube columns which are part of join chain
    Set<String> joinColumns = joinContext.getAllJoinPathColumnsOfTable((AbstractCubeTable) cubeql.getCube());
    if (joinColumns == null || joinColumns.isEmpty()) {
      return;
    }

    // Loop over all cube columns part of join paths
    for (String col : joinColumns) {
      CubeColumn column = cubeql.getCube().getColumnByName(col);
      for (TimeRange range : cubeql.getTimeRanges()) {
        if (!column.isColumnAvailableInTimeRange(range)) {
          log.info("Timerange queried is not in column life for {}, Removing join paths containing the column", column);
          // Remove join paths containing this column
          Map<Aliased<Dimension>, List<SchemaGraph.JoinPath>> allPaths = joinContext.getAllPaths();

          for (Aliased<Dimension> dimension : allPaths.keySet()) {
            List<SchemaGraph.JoinPath> joinPaths = allPaths.get(dimension);
            Iterator<SchemaGraph.JoinPath> joinPathIterator = joinPaths.iterator();

            while (joinPathIterator.hasNext()) {
              SchemaGraph.JoinPath path = joinPathIterator.next();
              if (path.containsColumnOfTable(col, (AbstractCubeTable) cubeql.getCube())) {
                log.info("Removing join path: {} as columns :{} is not available in the range", path, col);
                joinPathIterator.remove();
                if (joinPaths.isEmpty()) {
                  // This dimension doesn't have any paths left
                  throw new LensException(LensCubeErrorCode.NO_JOIN_PATH.getLensErrorInfo(),
                      "No valid join path available for dimension " + dimension + " which would satisfy time range "
                          + range.getFromDate() + "-" + range.getToDate());
                }
              }
            } // End loop to remove path

          } // End loop for all paths
        }
      } // End time range loop
    } // End column loop
  }


  private void throwException(CubeColumn column) throws ColUnAvailableInTimeRangeException {

    final Long availabilityStartTime = (column.getStartTimeMillisSinceEpoch().isPresent())
      ? column.getStartTimeMillisSinceEpoch().get() : null;

    final Long availabilityEndTime = column.getEndTimeMillisSinceEpoch().isPresent()
      ? column.getEndTimeMillisSinceEpoch().get() : null;

    ColUnAvailableInTimeRange col = new ColUnAvailableInTimeRange(column.getName(), availabilityStartTime,
      availabilityEndTime);

    throw new ColUnAvailableInTimeRangeException(col);
  }

  private void doFactRangeValidation(CubeQueryContext cubeql) {
    Iterator<CandidateFact> iter = cubeql.getCandidateFacts().iterator();
    while (iter.hasNext()) {
      CandidateFact cfact = iter.next();
      List<TimeRange> invalidTimeRanges = Lists.newArrayList();
      for (TimeRange timeRange : cubeql.getTimeRanges()) {
        if (!cfact.isValidForTimeRange(timeRange)) {
          invalidTimeRanges.add(timeRange);
        }
      }
      if (!invalidTimeRanges.isEmpty()){
        cubeql.addFactPruningMsgs(cfact.fact, CandidateTablePruneCause.factNotAvailableInRange(invalidTimeRanges));
        log.info("Not considering {} as it's not available for time ranges: {}", cfact, invalidTimeRanges);
        iter.remove();
      }
    }
    cubeql.pruneCandidateFactSet(FACT_NOT_AVAILABLE_IN_RANGE);
  }
}
