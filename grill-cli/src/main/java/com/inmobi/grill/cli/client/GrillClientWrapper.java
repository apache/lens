package com.inmobi.grill.cli.client;

import com.inmobi.grill.client.GrillClient;
import com.inmobi.grill.client.exceptions.GrillClientServerConnectionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Console;
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
public enum GrillClientWrapper {
  INSTANCE;
  private Log LOG = LogFactory.getLog(GrillClientWrapper.class);
  private GrillClient client;
  private String username;
  private String password;
  GrillClientWrapper() {
    try {
      client = new GrillClient();
    } catch(GrillClientServerConnectionException e) {
      if(e.getErrorCode() != 401) {
        explainFailedAttempt(e);
        throw e;
      }
      // Connecting without password prompt failed.
      for(int i = 0; i < 3; i++) {
        getCredentials();
        try{
          client = new GrillClient(username, password);
          break;
        } catch(GrillClientServerConnectionException grillClientServerConnectionException) {
          explainFailedAttempt(grillClientServerConnectionException);
          if(i == 2) {
            throw grillClientServerConnectionException;
          }
        }
      }
    }
  }
  public void explainFailedAttempt(GrillClientServerConnectionException e) {
    LOG.error("failed login attempt", e);
    switch(e.getErrorCode()) {
      case 401:
        System.console().printf("username/password combination incorrect.\n");
        break;
      case 500:
        System.console().printf("server unresponsive, Returned error code 500\n");
        break;
      default:
        System.console().printf("Unknown error in authenticating with the server. Error code = %d\n", e.getErrorCode());
    }
  }
  public void getCredentials(){
    Console console = System.console();
    if (console == null) {
      System.err.println("Couldn't get Console instance");
      System.exit(-1);
    }
    console.printf("username:");
    username = console.readLine().trim();
    char passwordArray[] = console.readPassword("password for %s:", username);
    password = new String(passwordArray);
  }

  public GrillClient getClient() {
    return client;
  }

  public void setClient(GrillClient client) {
    this.client = client;
  }

}
