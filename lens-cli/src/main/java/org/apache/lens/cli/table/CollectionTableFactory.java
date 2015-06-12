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

import java.util.Comparator;
import java.util.List;

import org.apache.lens.api.metastore.*;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class CollectionTableFactory {
  private CollectionTableFactory() {}

  public static CollectionTable<XFlattenedColumn> getCollectionTable(Class<? extends XField> claz, final String table) {
    if (claz == XExprColumn.class) {
      return new CollectionTable<>(Sets.newTreeSet(new Comparator<XFlattenedColumn>() {
        @Override
        public int compare(XFlattenedColumn o1, XFlattenedColumn o2) {
          return o1.getExpression().getName().compareTo(o2.getExpression().getName());
        }
      }),
        new CollectionTable.RowProvider<XFlattenedColumn>() {
          @Override
          public String[][] getRows(XFlattenedColumn element) {
            return new String[][]{
              {
                nulltoBlank(element.getExpression().getName()),
                nulltoBlank(element.getExpression().getDisplayString()),
                nulltoBlank(element.getExpression().getDescription()),
                expressionsAsString(element.getExpression().getExprSpec()),
              },
            };
          }

          private String expressionsAsString(List<XExprSpec> exprSpec) {
            StringBuilder sb = new StringBuilder();
            String sep = "";
            for (XExprSpec spec : exprSpec) {
              sb.append(sep);
              sep = ", ";
              List<String> clauses = Lists.newArrayList();
              if (spec.getStartTime() != null) {
                clauses.add("after " + spec.getStartTime());
              }
              if (spec.getEndTime() != null) {
                clauses.add("before " + spec.getEndTime());
              }
              String sep1 = "";
              if (clauses.isEmpty()) {
                clauses.add("always valid");
              }
              for (String clause : clauses) {
                sb.append(sep1).append(clause);
                sep1 = " and ";
              }
              sb.append(": ");
              sb.append(spec.getExpr());
            }
            return sb.toString();
          }
        }, "Name", "Display String", "Description", "Expr Specs");
    } else if (claz == XDimAttribute.class) {
      return new CollectionTable<>(Sets.newTreeSet(new Comparator<XFlattenedColumn>() {
        @Override
        public int compare(XFlattenedColumn o1, XFlattenedColumn o2) {
          if (o1 == null || o1.getDimAttribute() == null) {
            return -1;
          } else if (o2 == null || o2.getDimAttribute() == null) {
            return 1;
          } else if (table.equals(o1.getTableName()) && !table.equals(o2.getTableName())) {
            return -1;
          } else if (table.equals(o2.getTableName()) && !table.equals(o1.getTableName())) {
            return 1;
          } else {
            if (o1.getTableName() == null) {
              o1.setTableName("");
            }
            if (o2.getTableName() == null) {
              o2.setTableName("");
            }
            if (o1.getChainName() == null) {
              o1.setChainName("");
            }
            if (o2.getChainName() == null) {
              o2.setChainName("");
            }
            int cmp = o1.getTableName().compareTo(o2.getTableName());
            if (cmp != 0) {
              return cmp;
            }
            cmp = o1.getChainName().compareTo(o2.getChainName());
            if (cmp != 0) {
              return cmp;
            }
            return o1.getDimAttribute().getName().compareTo(o2.getDimAttribute().getName());
          }
        }
      }),
        new CollectionTable.RowProvider<XFlattenedColumn>() {
          @Override
          public String[][] getRows(XFlattenedColumn element) {
            String prefix = XFlattenedColumnTable.firstNonNull(element.getChainName(), element.getTableName());
            return new String[][]{
              {
                (prefix == null || prefix.isEmpty() || prefix.equalsIgnoreCase(table) ? "" : (prefix + "."))
                  + nulltoBlank(element.getDimAttribute().getName()),
                nulltoBlank(element.getDimAttribute().getDisplayString()),
                nulltoBlank(element.getDimAttribute().getDescription()),
              },
            };
          }
        }, "Name", "Display String", "Description");
    } else if (claz == XMeasure.class) {
      return new CollectionTable<>(Sets.newTreeSet(new Comparator<XFlattenedColumn>() {
        @Override
        public int compare(XFlattenedColumn o1, XFlattenedColumn o2) {
          return o1.getMeasure().getName().compareTo(o2.getMeasure().getName());
        }
      }),
        new CollectionTable.RowProvider<XFlattenedColumn>() {
          @Override
          public String[][] getRows(XFlattenedColumn element) {
            return new String[][]{
              {
                nulltoBlank(element.getMeasure().getName()),
                nulltoBlank(element.getMeasure().getDisplayString()),
                nulltoBlank(element.getMeasure().getDescription()),
              },
            };
          }
        }, "Name", "Display String", "Description");
    } else {
      return null;
    }
  }

  public static String nulltoBlank(String s) {
    return s == null ? "" : s;
  }
}
