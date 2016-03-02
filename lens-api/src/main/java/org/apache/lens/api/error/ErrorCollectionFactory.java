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
package org.apache.lens.api.error;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Creates ErrorCollection from error configuration file.
 */
public class ErrorCollectionFactory {

  private static final String LENS_ERROR_FILE_NAME = "lens-errors";
  private static final String LENS_ERRORS_OVERRIDE_FILE_NAME = "lens-errors-override";
  private static final String ERRORS_ARRAY_NAME = "errors";
  private static final String ERROR_CODE_KEY = "errorCode";
  private static final String HTTP_STATUS_CODE_KEY = "httpStatusCode";
  private static final String ERROR_MSG_KEY = "errorMsg";
  private static final String PAYLOAD_CLASS_KEY = "payloadClass";

  /**
   * Creates a new ErrorCollection from lens-errors.conf file overriding it with lens-errors-override.conf file.
   *
   * @return ErrorCollection instance created from error configuration file.
   * @throws ClassNotFoundException when error payload class defined in an error object in error configuration file
   * is not present in classpath.
   */
  public ErrorCollection createErrorCollection() throws ClassNotFoundException {

    Map<Integer, LensError> errorCollection = new HashMap<Integer, LensError>();

    Config baseConfig = ConfigFactory.load(LENS_ERROR_FILE_NAME);
    Config usedConfig = ConfigFactory.load(LENS_ERRORS_OVERRIDE_FILE_NAME).withFallback(baseConfig);
    List<? extends Config> configList = usedConfig.getConfigList(ERRORS_ARRAY_NAME);

    for (final Config config : configList) {

      int errorCode = config.getInt(ERROR_CODE_KEY);
      int httpStatusCodeInt = config.getInt(HTTP_STATUS_CODE_KEY);


      Response.StatusType httpStatusCode = LensHttpStatus.fromStatusCode(httpStatusCodeInt);
      String errorMsg = config.getString(ERROR_MSG_KEY);

      Class payloadClass = null;
      if (config.hasPath(PAYLOAD_CLASS_KEY)) {
        String payloadClassStr = config.getString(PAYLOAD_CLASS_KEY);
        payloadClass = Class.forName(payloadClassStr);
      }

      LensError lensError = new LensError(errorCode, httpStatusCode, errorMsg, Optional.fromNullable(payloadClass));
      errorCollection.put(errorCode, lensError);
    }

    ImmutableMap immutableMap = ImmutableMap.copyOf(errorCollection);
    return new ErrorMap(immutableMap);
  }
}
