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

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Writes partitions queried in timerange as between clause.
 * 
 */
public class BetweenTimeRangeWriter implements TimeRangeWriter {

  @Override
  public String getTimeRangeWhereClause(CubeQueryContext cubeQueryContext, String tableName,
      Set<FactPartition> rangeParts) throws SemanticException {
    if (rangeParts.size() == 0) {
      return "";
    }
    StringBuilder partStr = new StringBuilder();
    if (rangeParts.size() == 1) {
      partStr.append("(");
      String partFilter =
          TimeRangeUtils.getTimeRangePartitionFilter(rangeParts.iterator().next(), cubeQueryContext, tableName);
      partStr.append(partFilter);
      partStr.append(")");
    } else {
      TreeSet<FactPartition> parts = new TreeSet<FactPartition>();
      FactPartition first = null;
      Iterator<FactPartition> it = rangeParts.iterator();
      while (it.hasNext()) {
        FactPartition part = it.next();
        if (part.hasContainingPart()) {
          throw new SemanticException(ErrorMsg.CANNOT_USE_TIMERANGE_WRITER, "Partition has containing part");
        }
        if (first == null) {
          first = part;
        } else {
          // validate partcol, update period are same for both
          if (!first.getPartCol().equalsIgnoreCase(part.getPartCol())) {
            throw new SemanticException(ErrorMsg.CANNOT_USE_TIMERANGE_WRITER,
                "Part columns are different in partitions");
          }
          if (!first.getPeriod().equals(part.getPeriod())) {
            throw new SemanticException(ErrorMsg.CANNOT_USE_TIMERANGE_WRITER,
                "Partitions are in different update periods");
          }
        }
        parts.add(part);
      }

      FactPartition start = parts.first();
      FactPartition end = parts.last();

      String partCol = start.getPartCol();
      if (cubeQueryContext != null && !cubeQueryContext.shouldReplaceTimeDimWithPart()) {
        partCol = cubeQueryContext.getTimeDimOfPartitionColumn(partCol);
      }

      partStr.append(" (").append(tableName).append(".").append(partCol).append(" BETWEEN '")
          .append(start.getFormattedPartSpec()).append("' AND '").append(end.getFormattedPartSpec()).append("') ");
    }
    return partStr.toString();
  }
}
