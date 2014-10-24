package org.apache.lens.server.user;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.lens.server.api.LensConfConstants;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * The Class CustomUserConfigLoader.
 */
public class CustomUserConfigLoader extends UserConfigLoader {

  /** The custom handler class. */
  Class<? extends UserConfigLoader> customHandlerClass;

  /** The custom provider. */
  UserConfigLoader customProvider;

  /**
   * Instantiates a new custom user config loader.
   *
   * @param conf
   *          the conf
   */
  public CustomUserConfigLoader(HiveConf conf) {
    super(conf);
    this.customHandlerClass = (Class<? extends UserConfigLoader>) hiveConf.getClass(
        LensConfConstants.USER_RESOLVER_CUSTOM_CLASS, UserConfigLoader.class);
    try {
      this.customProvider = customHandlerClass.getConstructor(HiveConf.class).newInstance(conf);
      // in java6, these four extend directly from Exception. So have to handle separately. In java7,
      // the common subclass is ReflectiveOperationException
    } catch (InvocationTargetException e) {
      throw new UserConfigLoaderException(e);
    } catch (NoSuchMethodException e) {
      throw new UserConfigLoaderException(e);
    } catch (InstantiationException e) {
      throw new UserConfigLoaderException(e);
    } catch (IllegalAccessException e) {
      throw new UserConfigLoaderException(e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.user.UserConfigLoader#getUserConfig(java.lang.String)
   */
  @Override
  public Map<String, String> getUserConfig(String loggedInUser) throws UserConfigLoaderException {
    return customProvider.getUserConfig(loggedInUser);
  }
}
