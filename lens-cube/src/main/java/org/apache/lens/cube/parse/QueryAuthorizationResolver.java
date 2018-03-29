package org.apache.lens.cube.parse;

import java.util.*;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.*;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.authorization.ActionType;
import org.apache.lens.server.api.authorization.IAuthorizer;
import org.apache.lens.server.api.authorization.LensPrivilegeObject;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import lombok.Getter;

public class QueryAuthorizationResolver implements ContextRewriter {

  @Getter
  private IAuthorizer authorizer;
  @Getter
  private Boolean isAuthorizationCheckEnabled;

  QueryAuthorizationResolver(Configuration conf) {
    authorizer = ReflectionUtils.newInstance(
      conf.getClass(MetastoreConstants.AUTHORIZER_CLASS, LensConfConstants.DEFAULT_AUTHORIZER, IAuthorizer.class), conf);
    isAuthorizationCheckEnabled = conf.getBoolean(LensConfConstants.ENABLE_AUTHORIZATION_CHECK,
      LensConfConstants.DEFAULT_ENABLE_AUTHORIZATION_CHECK);
  }
  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {

    if(isAuthorizationCheckEnabled) {
      for (Map.Entry<String, Set<String>> entry : cubeql.getTblAliasToColumns().entrySet()) {
        String alias = entry.getKey();
        // skip default alias
        if (Objects.equals(alias, CubeQueryContext.DEFAULT_TABLE)) {
          continue;
        }
        AbstractCubeTable tbl = cubeql.getCubeTableForAlias(alias);
        Set<String> columns = entry.getValue();

        Set<String> sensitiveFields = ((AbstractBaseTable) tbl).getSensitiveColumnsFromQuery(columns);
        for (String col : sensitiveFields) {
          if (!getAuthorizer().authorize(new LensPrivilegeObject(LensPrivilegeObject.LensPrivilegeObjectType.COLUMN, tbl.getName(), col), ActionType.SELECT,
            new HashSet<>(Arrays.asList(cubeql.getConf().get(LensConfConstants.SESSION_USER_GROUPS).split(","))))) {
            throw new LensException(LensCubeErrorCode.NOT_AUTHORIZED_EXCEPTION.getLensErrorInfo());
          }
        }
      }
    }
  }
}
