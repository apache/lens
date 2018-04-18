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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Set;

import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.common.ConfigBasedObjectCreationFactory;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility methods for Lens
 */
@Slf4j
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
  public static String getCauseMessage(@NonNull Throwable e) {
    String expMsg = null;
    if (e.getCause() != null) {
      expMsg = getCauseMessage(e.getCause());
    }
    if (StringUtils.isBlank(expMsg)) {
      expMsg = e.getLocalizedMessage();
    }
    return expMsg;
  }

  public static Throwable getCause(@NonNull Throwable e) {
    if (e.getCause() != null) {
      return getCause(e.getCause());
    }
    return e;
  }

  public static boolean isSocketException(@NonNull Throwable e) {
    Throwable cause = getCause(e);
    return cause instanceof SocketException || cause instanceof SocketTimeoutException;
  }
  public static <T> Set<T> getImplementationsMutable(final String factoriesKey, final Configuration conf) {
    Set<T> implSet = Sets.newLinkedHashSet();
    final String[] factoryNames = conf.getStrings(factoriesKey);
    if (factoryNames != null) {
      for (String factoryName : factoryNames) {
        if (StringUtils.isNotBlank(factoryName)) {
          final T implementation = getImplementation(factoryName.trim(), conf);
          implSet.add(implementation);
        }
      }
    }
    return implSet;
  }
  public static <T> ImmutableSet<T> getImplementations(final String factoriesKey, final Configuration conf) {
    Set<T> implSet = getImplementationsMutable(factoriesKey, conf);
    return ImmutableSet.copyOf(implSet);
  }

  public static <T> T getImplementation(final String factoryName, final Configuration conf) {

    try {
      ConfigBasedObjectCreationFactory<T> factory
        = (ConfigBasedObjectCreationFactory<T>) Class.forName(factoryName).newInstance();
      T ret = factory.create(conf);
      if (ret instanceof Configurable) {
        ((Configurable) ret).setConf(conf);
      }
      return ret;
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

  private static void checkIsReadable(String keytabFilePath) {
    File keytabFile = new File(keytabFilePath);
    if (!keytabFile.exists()) {
      throw new IllegalArgumentException("The keytab file does not exist! " + keytabFilePath);
    }

    if (!keytabFile.isFile()) {
      throw new IllegalArgumentException("The keytab file cannot be a directory! " + keytabFilePath);
    }

    if (!keytabFile.canRead()) {
      throw new IllegalArgumentException("The keytab file is not readable! " + keytabFilePath);
    }
  }

  public static void refreshLensTGT(HiveConf conf) throws IOException, IllegalArgumentException {

    String principalString = conf.get(LensConfConstants.LENS_PRINCIPAL);

    Validate.notEmpty(principalString,
            "Missing required configuration property: " + LensConfConstants.LENS_PRINCIPAL);

    String principal = SecurityUtil.getServerPrincipal(
            principalString,
            InetAddress.getLocalHost().getCanonicalHostName());

    String keytabFilePath = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB);

    Validate.notEmpty(keytabFilePath, "Missing required configuration property: "
                    + HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB.toString());
    checkIsReadable(keytabFilePath);

    Configuration hadoopConf = new Configuration();
    hadoopConf.set("hadoop.security.authentication", "kerberos");

    UserGroupInformation.setConfiguration(hadoopConf);

    UserGroupInformation.loginUserFromKeytab(principal, keytabFilePath);

    log.info("RetryingThriftCLIServiceClientSasl : Got Kerberos ticket, keytab: {}, Lens principal: {}",
            keytabFilePath, principal);

  }
}
