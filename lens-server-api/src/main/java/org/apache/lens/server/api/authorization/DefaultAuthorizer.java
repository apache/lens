package org.apache.lens.server.api.authorization;

import java.util.Collection;
import java.util.Set;

/**
 * Created by rajithar on 21/2/18.
 */
public class DefaultAuthorizer implements IAuthorizer {

  @Override
  public boolean authorize(LensPrivilegeObject lensPrivilegeObject, ActionType accessType,  Collection<String> userGroups) {
    return false;
  }
}
