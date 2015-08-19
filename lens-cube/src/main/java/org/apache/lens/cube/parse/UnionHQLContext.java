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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
public abstract class UnionHQLContext implements HQLContextInterface {

  @Getter
  @Setter
  List<HQLContextInterface> hqlContexts = new ArrayList<HQLContextInterface>();

  @Override
  public String toHQL() throws LensException {
    Set<String> queryParts = new LinkedHashSet<String>();
    for (HQLContextInterface ctx : hqlContexts) {
      queryParts.add(ctx.toHQL());
    }
    return StringUtils.join(queryParts, " UNION ALL ");
  }

  @Override
  public String getSelect()  {
    throw new NotImplementedException("Not Implemented");
  }

  @Override
  public String getFrom() {
    throw new NotImplementedException("Not Implemented");
  }

  @Override
  public String getWhere() {
    throw new NotImplementedException("Not Implemented");
  }

  @Override
  public String getGroupby() {
    throw new NotImplementedException("Not Implemented");
  }

  @Override
  public String getHaving() {
    throw new NotImplementedException("Not Implemented");
  }

  @Override
  public String getOrderby() {
    throw new NotImplementedException("Not Implemented");
  }

  @Override
  public Integer getLimit() {
    throw new NotImplementedException("Not Implemented");
  }

}
