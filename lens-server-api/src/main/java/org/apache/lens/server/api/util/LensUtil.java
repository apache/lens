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
package org.apache.lens.server.api.util;

import java.util.HashMap;
import java.util.Set;

import org.apache.lens.server.api.common.ConfigBasedObjectCreationFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Utility methods for Lens
 */
public final class LensUtil {

  private LensUtil() {

  }

  /**
   * Get the message corresponding to base cause. If no cause is available or no message is available
   *  parent's message is returned.
   *
   * @param e
   * @return message
   */
  public static String getCauseMessage(Throwable e) {
    String expMsg = null;
    if (e.getCause() != null) {
      expMsg = getCauseMessage(e.getCause());
    }
    if (StringUtils.isBlank(expMsg)) {
      expMsg = e.getLocalizedMessage();
    }
    return expMsg;
  }

  public static <T> ImmutableSet<T> getImplementations(final String factoriesKey, final Configuration conf) {

    Set<T> implSet = Sets.newLinkedHashSet();
    final String[] factoryNames = conf.getStrings(factoriesKey);

    if (factoryNames == null) {
      return ImmutableSet.copyOf(implSet);
    }

    for (String factoryName : factoryNames) {
      if (StringUtils.isNotBlank(factoryName)) {
        final T implementation = getImplementation(factoryName, conf);
        implSet.add(implementation);
      }
    }
    return ImmutableSet.copyOf(implSet);
  }

  public static <T> T getImplementation(final String factoryName, final Configuration conf) {

    try {
      ConfigBasedObjectCreationFactory<T> factory
        = (ConfigBasedObjectCreationFactory<T>) Class.forName(factoryName).newInstance();
      return factory.create(conf);
    } catch (final ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  public static <K, V> HashMap<K, V> getHashMap(Object... args) {
    assert (args.length % 2 == 0);
    HashMap<K, V> map = new HashMap<>();
    for (int i = 0; i < args.length; i += 2) {
      map.put((K) args[i], (V) args[i + 1]);
    }
    return map;
  }
}
