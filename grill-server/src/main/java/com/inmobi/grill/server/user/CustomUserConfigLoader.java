package com.inmobi.grill.server.user;
/*
 * #%L
 * Grill Server
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.inmobi.grill.server.api.GrillConfConstants;
import org.apache.hadoop.hive.conf.HiveConf;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

public class CustomUserConfigLoader extends UserConfigLoader {

  Class<? extends UserConfigLoader> customHandlerClass;
  UserConfigLoader customProvider;

  public CustomUserConfigLoader(HiveConf conf) {
    super(conf);
    this.customHandlerClass = (Class<? extends UserConfigLoader>) hiveConf.getClass(
      GrillConfConstants.GRILL_SERVER_USER_RESOLVER_CUSTOM_CLASS,
      UserConfigLoader.class
    );
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

  @Override
  public Map<String, String> getUserConfig(String loggedInUser) throws UserConfigLoaderException {
    return customProvider.getUserConfig(loggedInUser);
  }
}
