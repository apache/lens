package com.inmobi.grill.client;

import java.io.Console;

/**
 * User: rajat.khandelwal
 * Date: 16/09/14
 */
public class Credentials {
  public String username;
  public String password;
  public Credentials(String username, String password) {
    this.username = username;
    this.password = password;
  }
  public static Credentials prompt() {
    Console console = System.console();
    if (console == null) {
      System.err.println("Couldn't get Console instance");
      System.exit(-1);
    }
    console.printf("username:");
    String username = console.readLine().trim();
    char passwordArray[] = console.readPassword("password for %s:", username);
    String password = new String(passwordArray);
    return new Credentials(username, password);
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }
}
