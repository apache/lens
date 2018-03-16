package org.apache.lens.cube.parse;

import org.apache.lens.server.api.error.LensException;

public class QueryAuthorizationResolver implements ContextRewriter {
  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
      //TODO Get the sensitive fields (from cube properties), get the intersection with columns queried and call authorization check here

  }
}
