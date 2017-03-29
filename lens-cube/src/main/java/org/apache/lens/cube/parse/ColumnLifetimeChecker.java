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

import org.apache.lens.cube.error.ColUnAvailableInTimeRange;
import org.apache.lens.cube.error.ColUnAvailableInTimeRangeException;
import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.*;
import org.apache.lens.cube.metadata.join.JoinPath;
import org.apache.lens.cube.parse.join.AutoJoinContext;
import org.apache.lens.server.api.error.LensException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ColumnLifetimeChecker implements ContextRewriter {
  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
    if (cubeql.getCube() == null) {
      return;
    }
    doColLifeValidation(cubeql);
  }

  private void doColLifeValidation(CubeQueryContext cubeql) throws LensException,
      ColUnAvailableInTimeRangeException {
    Set<String> cubeColumns = cubeql.getColumnsQueriedForTable(cubeql.getCube().getName());
    if (cubeColumns == null || cubeColumns.isEmpty()) {
      // Query doesn't have any columns from cube
      return;
    }

    for (String col : cubeql.getColumnsQueriedForTable(cubeql.getCube().getName())) {
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
          Map<Aliased<Dimension>, List<JoinPath>> allPaths = joinContext.getAllPaths();

          for (Aliased<Dimension> dimension : allPaths.keySet()) {
            List<JoinPath> joinPaths = allPaths.get(dimension);
            Iterator<JoinPath> joinPathIterator = joinPaths.iterator();

            while (joinPathIterator.hasNext()) {
              JoinPath path = joinPathIterator.next();
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
}
