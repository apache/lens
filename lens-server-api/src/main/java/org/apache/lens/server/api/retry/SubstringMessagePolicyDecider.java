/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.api.retry;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.lens.api.util.CommonUtils;
import org.apache.lens.server.api.LensConfConstants;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SubstringMessagePolicyDecider<FC extends FailureContext> implements RetryPolicyDecider<FC>, Configurable {
  private Configuration conf = null;
  private Map<String, Constructor> constructorMap = new HashMap<>();
  private Map<String, String[]> argsMap = new HashMap<>();

  @Override
  public BackOffRetryHandler<FC> decidePolicy(String errorMessage) {
    if (errorMessage == null) {
      return null;
    }
    for (Map.Entry<String, Constructor> entry : constructorMap.entrySet()) {
      String key = entry.getKey();
      Constructor constructor = entry.getValue();
      if (errorMessage.contains(key)) {
        try {
          return (BackOffRetryHandler) constructor.newInstance(argsMap.get(key));
        } catch (InstantiationException e) {
          log.warn("Instantiation Error ", e);
        } catch (IllegalAccessException e) {
          log.warn("Illegal Access Exception ", e);
        } catch (InvocationTargetException e) {
          log.warn("Invocation Target Exception ", e);
        }
      }
    }
    return null;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    if (this.conf != null) {
      String mapString = conf.get(LensConfConstants.RETRY_MESSAGE_MAP);
      Map<String, String> errorMessageMap = CommonUtils.parseMapFromString(mapString);
      // For all the values get the constructors.
      // The assumption here is that the policies will have few constructors with all string parameters.
      for (Map.Entry<String, String> entry : errorMessageMap.entrySet()) {
        String val = entry.getValue();
        String className = val.substring(0, val.indexOf("("));
        String paramString = val.substring(val.indexOf("(") + 1, val.length() - 1);
        String[] args = paramString.split(" ");
        Class[] constructorTypes = new Class[args.length];
        Arrays.fill(constructorTypes, String.class);
        try {
          constructorMap.put(entry.getKey(), Class.forName(className).getConstructor(constructorTypes));
          argsMap.put(entry.getKey(), args);
        } catch (ClassNotFoundException e) {
          log.warn("Class not found", e);
        } catch (NoSuchMethodException e) {
          log.warn("No method found", e);
        }
      }
    }
  }
}

