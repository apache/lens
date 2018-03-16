package org.apache.lens.cube.parse;

import org.apache.lens.server.api.error.LensException;

/**
 * Created by rajithar on 6/2/18.
 */
public class AccessGroupChecker  implements  ContextRewriter {
  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
    checkAllColumnsAccessible(cubeql);
  }

  private void checkAllColumnsAccessible(CubeQueryContext cubeql) {
    //for(cubeql.getColumnsQueried())
  }
}
