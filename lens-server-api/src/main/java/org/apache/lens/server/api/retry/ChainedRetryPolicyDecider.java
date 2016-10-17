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
package org.apache.lens.server.api.retry;

import java.util.List;

import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;
import lombok.Data;

@Data
public class ChainedRetryPolicyDecider<FC extends FailureContext> implements RetryPolicyDecider<FC> {
  private final Iterable<RetryPolicyDecider<FC>> policyDeciders;

  @Override
  public BackOffRetryHandler<FC> decidePolicy(String errorMessage) {
    for (RetryPolicyDecider<FC> policyDecider : policyDeciders) {
      BackOffRetryHandler<FC> policy = policyDecider.decidePolicy(errorMessage);
      if (policy != null) {
        return policy;
      }
    }
    return new NoRetryHandler<>();
  }
  public static <FC extends FailureContext> ChainedRetryPolicyDecider<FC> from(Configuration conf, String key)
    throws LensException {
    String[] classNames = conf.getStrings(key);
    List<RetryPolicyDecider<FC>> retryPolicyDeciders = Lists.newArrayList();
    if (classNames != null) {
      for (String className: classNames) {
        Class<? extends RetryPolicyDecider<FC>> clazz;
        try {
          clazz = (Class<? extends RetryPolicyDecider<FC>>) conf.getClassByName(className)
            .asSubclass(RetryPolicyDecider.class);
        } catch (ClassNotFoundException e) {
          throw new LensException("Couldn't load class " + className, e);
        }
        RetryPolicyDecider<FC> instance;
        try {
          instance = clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
          throw new LensException("Couldn't create instance of class " + clazz.getName(), e);
        }
        if (instance instanceof Configurable) {
          ((Configurable) instance).setConf(conf);
        }
        retryPolicyDeciders.add(instance);
      }
    }
    return new ChainedRetryPolicyDecider<>(retryPolicyDeciders);
  }
}
