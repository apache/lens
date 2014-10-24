package org.apache.lens.server.auth;

import org.apache.hive.service.auth.PasswdAuthenticationProvider;

import javax.security.sasl.AuthenticationException;

/**
 * The Class FooBarAuthenticationProvider.
 */
public class FooBarAuthenticationProvider implements PasswdAuthenticationProvider {

  /** The msg. */
  public static String MSG = "<username,password>!=<foo@localhost,bar>";

  /** The allowed combinations. */
  private final String[][] allowedCombinations = new String[][] { { "foo", "bar" }, { "anonymous", "" }, };

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hive.service.auth.PasswdAuthenticationProvider#Authenticate(java.lang.String, java.lang.String)
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
}
