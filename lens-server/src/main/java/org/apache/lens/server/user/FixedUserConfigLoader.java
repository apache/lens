package org.apache.lens.server.user;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.lens.server.api.LensConfConstants;

import java.util.HashMap;
import java.util.Map;

/**
 * The Class FixedUserConfigLoader.
 */
public class FixedUserConfigLoader extends UserConfigLoader {

  /**
   * Instantiates a new fixed user config loader.
   *
   * @param conf
   *          the conf
   */
  public FixedUserConfigLoader(HiveConf conf) {
    super(conf);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.user.UserConfigLoader#getUserConfig(java.lang.String)
   */
  @Override
  public Map<String, String> getUserConfig(String loggedInUser) {
    HashMap<String, String> userConfig = new HashMap<String, String>();
    userConfig.put(LensConfConstants.SESSION_CLUSTER_USER, hiveConf.get(LensConfConstants.USER_RESOLVER_FIXED_VALUE));
    return userConfig;
  }
}
