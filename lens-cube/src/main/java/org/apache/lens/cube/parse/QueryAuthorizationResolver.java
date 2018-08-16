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

import java.util.*;

import org.apache.lens.cube.authorization.AuthorizationUtil;
import org.apache.lens.cube.metadata.*;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.authorization.ActionType;
import org.apache.lens.server.api.authorization.Authorizer;
import org.apache.lens.server.api.authorization.LensPrivilegeObject;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryAuthorizationResolver implements ContextRewriter {

  @Getter
  private Authorizer authorizer;
  @Getter
  private Boolean isAuthorizationCheckEnabled;

  QueryAuthorizationResolver(Configuration conf) {
    isAuthorizationCheckEnabled = conf.getBoolean(LensConfConstants.ENABLE_QUERY_AUTHORIZATION_CHECK,
      LensConfConstants.DEFAULT_ENABLE_QUERY_AUTHORIZATION_CHECK);
    authorizer = ReflectionUtils.newInstance(
      conf.getClass(MetastoreConstants.AUTHORIZER_CLASS, LensConfConstants.DEFAULT_AUTHORIZER, Authorizer.class),
      conf);
  }
  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {

    log.info("==> Query Authorization enabled : "+ isAuthorizationCheckEnabled);
    if (isAuthorizationCheckEnabled) {
      for (Map.Entry<String, Set<String>> entry : cubeql.getTblAliasToColumns().entrySet()) {
        String alias = entry.getKey();
        // skip default alias
        if (Objects.equals(alias, CubeQueryContext.DEFAULT_TABLE)) {
          continue;
        }
        AbstractCubeTable tbl = cubeql.getCubeTableForAlias(alias);
        Set<String> queriedColumns = entry.getValue();

        Set<String> restrictedFieldsQueried =
          getRestrictedColumnsFromQuery(((AbstractBaseTable) tbl).getRestrictedColumns(), queriedColumns);
        log.info("Restricted queriedColumns queried : "+ restrictedFieldsQueried);
        if (restrictedFieldsQueried != null && !restrictedFieldsQueried.isEmpty()) {
          for (String col : restrictedFieldsQueried) {
            AuthorizationUtil.isAuthorized(getAuthorizer(), tbl.getName(), col,
              LensPrivilegeObject.LensPrivilegeObjectType.COLUMN, ActionType.SELECT, cubeql.getConf());
          }
        }
      }
    }
    log.info("<== Query Authorization enabled : "+ isAuthorizationCheckEnabled);
  }

  /*
  * Returns the intersection of queried and restricted columns of table
  * */
  private static Set<String> getRestrictedColumnsFromQuery(Set<String> restrictedColumns, Set<String> queriedColumns){
    restrictedColumns.retainAll(queriedColumns);
    return restrictedColumns;
  }
}
