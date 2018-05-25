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

import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import lombok.extern.slf4j.Slf4j;

/**
 * LensTrustManager : class to instantiate trust manager for lens client
 * and verify server certs.
 */
@Slf4j
public class LensTrustManager implements X509TrustManager {

  private boolean ignoreCertCheck;
  private X509TrustManager trustManager;

  public LensTrustManager(LensClientConfig config) {

    if (Boolean.valueOf(config.get(LensClientConfig.SSL_IGNORE_SERVER_CERT,
            String.valueOf(LensClientConfig.DEFAULT_SSL_IGNORE_SERVER_CERT_VALUE)))) {
      log.debug("Will skip server cert verification.");
      ignoreCertCheck = true;
    } else {
      log.debug("Server cert verification is enabled.");
      ignoreCertCheck = false;
      try {
        trustManager = getTrustManager();
      } catch (Exception e) {
        log.error(e.toString());
        throw new RuntimeException(e);
      }
    }

  }

  /**
   *
   * @param chain
   * @param authType
   * @throws CertificateException
   */
  @Override
  public void checkClientTrusted(final X509Certificate[] chain, final String authType) throws CertificateException {
  }

  /**
   *
   * @param chain
   * @param authType
   * @throws CertificateException
   */
  @Override
  public void checkServerTrusted(final X509Certificate[] chain, final String authType) throws CertificateException {
  }

  /**
   *
   * @return
   */
  @Override
  public X509Certificate[] getAcceptedIssuers() {
    if (ignoreCertCheck) {
      log.debug("return root X509.");
      return new X509Certificate[0];
    } else {
      log.debug("return first CA X509 cert.");
      return trustManager.getAcceptedIssuers();
    }
  }

  /**
   *
   * @return trust manager to init trust chain
   * @throws Exception
   */
  private X509TrustManager getTrustManager() throws Exception {

    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init((KeyStore) null);

    X509TrustManager x509Tm = null;

    for (TrustManager tm : tmf.getTrustManagers()) {
      if (tm instanceof X509TrustManager) {
        x509Tm = (X509TrustManager) tm;
        break;
      }
    }
    return  x509Tm;
  }
}
