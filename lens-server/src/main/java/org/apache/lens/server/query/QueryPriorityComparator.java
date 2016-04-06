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

import org.apache.lens.api.Priority;
import org.apache.lens.server.api.query.QueryContext;

public class QueryPriorityComparator extends FIFOQueryComparator {

  @Override
  public int compare(final QueryContext o1, final QueryContext o2) {

    Priority pO1 = o1.getPriority();
    Priority pO2 = o2.getPriority();

    int result = pO1.compareTo(pO2);
    if (result == 0) {
      return super.compare(o1, o2);
    }
    return result;
  }
}
