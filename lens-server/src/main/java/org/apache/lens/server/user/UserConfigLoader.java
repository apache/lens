package org.apache.lens.server.user;

import org.apache.hadoop.hive.conf.HiveConf;

import java.util.Map;

/**
 * The Class UserConfigLoader.
 */
public abstract class UserConfigLoader {

  /** The hive conf. */
  protected final HiveConf hiveConf;

  /**
   * Instantiates a new user config loader.
   *
   * @param conf
   *          the conf
   */
  protected UserConfigLoader(HiveConf conf) {
    this.hiveConf = conf;
  }

  /**
   * Gets the user config.
   *
   * @param loggedInUser
   *          the logged in user
   * @return the user config
   * @throws UserConfigLoaderException
   *           the user config loader exception
   */
  public abstract Map<String, String> getUserConfig(String loggedInUser) throws UserConfigLoaderException;
}
