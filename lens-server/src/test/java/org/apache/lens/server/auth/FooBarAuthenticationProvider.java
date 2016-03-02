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
package org.apache.lens.server.auth;

import javax.security.sasl.AuthenticationException;

import org.apache.hive.service.auth.PasswdAuthenticationProvider;

/**
 * The Class FooBarAuthenticationProvider.
 */
public class FooBarAuthenticationProvider implements PasswdAuthenticationProvider {

  /** The msg. */
  public static final String MSG = "<username,password>!=<foo@localhost,bar>";

  /** The allowed combinations. */
  private final String[][] allowedCombinations
    = new String[][]{{"foo", "bar"}, {"anonymous", ""}, {"test", "test"}, {"UITest", "UITest"}};

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hive.service.auth.PasswdAuthenticationProvider#Authenticate(java.lang.String, java.lang.String)
   * SUSPEND CHECKSTYLE CHECK MethodName
   */
  @Override
  public void Authenticate(String username, String password) throws AuthenticationException {
    for (String[] usernamePassword : allowedCombinations) {
      if (username.equals(usernamePassword[0]) && password.equals(usernamePassword[1])) {
        return;
      }
    }
    throw new AuthenticationException(MSG);
  }
  // RESUME CHECKSTYLE CHECK MethodName
}
