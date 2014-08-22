package com.inmobi.grill.server.auth;

import org.apache.hive.service.auth.PasswdAuthenticationProvider;

import javax.security.sasl.AuthenticationException;

/**
 * User: rajat.khandelwal
 * Date: 20/08/14
 */
public class FooBarAuthenticationProvider implements PasswdAuthenticationProvider {
  public static String MSG = "<username,password>!=<foo,bar>";
  @Override
  public void Authenticate(String username, String password) throws AuthenticationException {
    if(!(username.equals("foo") && password.equals("bar"))){
      throw new AuthenticationException(MSG);
    }
  }
}
