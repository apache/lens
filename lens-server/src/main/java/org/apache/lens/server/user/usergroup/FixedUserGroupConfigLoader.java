package org.apache.lens.server.user.usergroup;

import java.util.HashMap;
import java.util.Map;

import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.user.UserGroupConfigLoader;

import org.apache.hadoop.hive.conf.HiveConf;

import com.google.common.collect.Maps;

public class FixedUserGroupConfigLoader implements UserGroupConfigLoader {

  private final String fixedValue;

  public FixedUserGroupConfigLoader(HiveConf conf) {
    fixedValue = conf.get(LensConfConstants.USER_GROUP_FIXED_VALUE);
  }

  /*
     * (non-Javadoc)
     *
     * @see org.apache.lens.server.user.UserGroupConfigLoader#getUserConfig(java.lang.String)
     */
  @Override
  public Map<String, String> getUserConfig(String loggedInUser) {
    HashMap<String, String> userConfig = Maps.newHashMap();
    userConfig.put(LensConfConstants.SESSION_USER_GROUPS, fixedValue);
    return userConfig;
  }
}

