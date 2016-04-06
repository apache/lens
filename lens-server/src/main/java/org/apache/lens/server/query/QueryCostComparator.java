/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.query;

import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.cost.QueryCost;

public class QueryCostComparator extends FIFOQueryComparator {

  @Override
  public int compare(final QueryContext o1, final QueryContext o2) {

    QueryCost qcO1 = o1.getSelectedDriverQueryCost();
    QueryCost qcO2 = o2.getSelectedDriverQueryCost();

    int result = qcO1.compareTo(qcO2);
    if (result == 0) {
      return super.compare(o1, o2);
    }
    return result;
  }
}

