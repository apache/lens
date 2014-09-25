package com.inmobi.grill.server.auth;

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

import org.apache.hive.service.auth.PasswdAuthenticationProvider;

import javax.security.sasl.AuthenticationException;

public class FooBarAuthenticationProvider implements PasswdAuthenticationProvider {
  public static String MSG = "<username,password>!=<foo@localhost,bar>";
  private final String[][] allowedCombinations = new String[][]{
    {"foo", "bar"},
    {"anonymous", ""},
  };

  @Override
  public void Authenticate(String username, String password) throws AuthenticationException {
    for(String[] usernamePassword: allowedCombinations){
      if(username.equals(usernamePassword[0]) && password.equals(usernamePassword[1])){
        return;
      }
    }
    throw new AuthenticationException(MSG);
  }
}
