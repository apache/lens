package org.apache.lens.server.api.authorization;

import java.util.Set;

/**
 * Created by rajithar on 28/2/18.
 */
public interface UserGroupFetcher {

  public Set<String> getGroupList(String userName);
}
