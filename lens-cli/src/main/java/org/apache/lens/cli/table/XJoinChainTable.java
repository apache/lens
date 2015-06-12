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
package org.apache.lens.cli.table;


import static org.apache.lens.cli.table.CollectionTableFactory.nulltoBlank;

import org.apache.lens.api.metastore.*;

public class XJoinChainTable {

  private XJoinChains xJoinChains;

  public XJoinChainTable(XJoinChains xJoinChains) {
    this.xJoinChains = xJoinChains;
  }

  @Override
  public String toString() {
    return new CollectionTable<>(xJoinChains.getJoinChain(), new CollectionTable.RowProvider<XJoinChain>() {
      @Override
      public String[][] getRows(XJoinChain element) {
        int size = element.getPaths().getPath().size();
        String[][] ret = new String[size][5];
        for (int i = 0; i < size; i++) {
          if (i == 0) {
            ret[i][0] = nulltoBlank(element.getName());
            ret[i][1] = nulltoBlank(element.getDisplayString());
            ret[i][2] = nulltoBlank(element.getDescription());
            ret[i][3] = nulltoBlank(element.getDestTable());
          } else {
            ret[i][0] = "";
            ret[i][1] = "";
            ret[i][2] = "";
            ret[i][3] = "";
          }
          ret[i][4] = pathAsString(element.getPaths().getPath().get(i));
        }
        return ret;
      }

      private String pathAsString(XJoinPath path) {
        StringBuilder sb = new StringBuilder();
        String sep1 = "";
        for (XJoinEdge edge : path.getEdges().getEdge()) {
          sb.append(sep1)
            .append(edge.getFrom().getTable()).append(".").append(edge.getFrom().getColumn())
            .append("=")
            .append(edge.getTo().getTable()).append(".").append(edge.getTo().getColumn());
          sep1 = "->";
        }
        return sb.toString();
      }
    }

      , "Name", "Display String", "Description", "Destination Table", "Path").

      toString();
  }
}

