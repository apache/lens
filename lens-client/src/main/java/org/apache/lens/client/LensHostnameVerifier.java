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
package org.apache.lens.client;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;

import lombok.extern.slf4j.Slf4j;

/**
 * LensHostnameVerifier : Class to verify host name or cname mentioned in
 * lens server's base url is same as present in SSL cert.
 */
@Slf4j
public class LensHostnameVerifier implements HostnameVerifier {

  private boolean ignoreHostVerification;
  private String lensServerHostBaseURL;

  public LensHostnameVerifier(LensClientConfig config) {

    if (Boolean.valueOf(config.get(LensClientConfig.SSL_IGNORE_SERVER_CERT,
            String.valueOf(LensClientConfig.DEFAULT_SSL_IGNORE_SERVER_CERT_VALUE)))) {
      log.info("Will skip hostname verification.");
      ignoreHostVerification = true;
      lensServerHostBaseURL = config.get(LensClientConfig.SERVER_BASE_URL);
    } else {
      log.info("Host name verification is enabled.");
      ignoreHostVerification = false;
    }

  }

  @Override
  public boolean verify(String hostname, SSLSession session) {

    if (ignoreHostVerification) {
      return true;
    } else {
      return lensServerHostBaseURL.contains(hostname);
    }
  }
}
