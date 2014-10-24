package org.apache.lens.server.user;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.lens.server.user.UserConfigLoader;

import java.util.HashMap;
import java.util.Map;

/**
 * The Class FooBarConfigLoader.
 */
public class FooBarConfigLoader extends UserConfigLoader {

  /** The const hash map. */
  public static HashMap<String, String> CONST_HASH_MAP = new HashMap<String, String>() {
    {
      put("key", "value");
    }
  };

  /**
   * Instantiates a new foo bar config loader.
   *
   * @param conf
   *          the conf
   */
  public FooBarConfigLoader(HiveConf conf) {
    super(conf);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.user.UserConfigLoader#getUserConfig(java.lang.String)
   */
  @Override
  public Map<String, String> getUserConfig(String loggedInUser) {
    return CONST_HASH_MAP;
  }
}
