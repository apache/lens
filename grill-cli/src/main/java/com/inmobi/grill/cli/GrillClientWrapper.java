package com.inmobi.grill.cli;

/*
 * #%L
 * Grill client
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

import com.inmobi.grill.client.GrillClient;

import java.io.Console;

public class GrillClientWrapper {

  protected GrillClient client;
  protected String username;
  protected String password;

  public GrillClientWrapper() {
    getCredentials();
    client = new GrillClient(username, password);
  }

  public void getCredentials(){
    Console console = System.console();
    if (console == null) {
      System.err.println("Couldn't get Console instance");
      System.exit(-1);
    }
    console.printf("username:");
    username = console.readLine();
    char passwordArray[] = console.readPassword("password:");
    password = new String(passwordArray);
  }

  public GrillClient getClient() {
    return client;
  }

  public void setClient(GrillClient client) {
    this.client = client;
  }
}
