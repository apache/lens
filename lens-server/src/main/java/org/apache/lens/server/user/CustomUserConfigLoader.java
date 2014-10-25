/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
