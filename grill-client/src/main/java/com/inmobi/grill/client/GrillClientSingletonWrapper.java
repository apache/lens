package com.inmobi.grill.client;

import com.inmobi.grill.client.exceptions.GrillClientServerConnectionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
public enum GrillClientSingletonWrapper {
  INSTANCE;
  private Log LOG = LogFactory.getLog(GrillClientSingletonWrapper.class);
  private GrillClient client;
  private static final int MAX_RETRIES = 3;
  GrillClientSingletonWrapper() {
    try {
      client = new GrillClient();
    } catch(GrillClientServerConnectionException e) {
      if(e.getErrorCode() != 401) {
        explainFailedAttempt(e);
        throw e;
      }
      // Connecting without password prompt failed.
      for(int i = 0; i < MAX_RETRIES; i++) {
        try{
          client = new GrillClient(Credentials.prompt());
          break;
        } catch(GrillClientServerConnectionException grillClientServerConnectionException) {
          explainFailedAttempt(grillClientServerConnectionException);
          if(i == MAX_RETRIES - 1) {
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

  public GrillClient getClient() {
    return client;
  }

  public void setClient(GrillClient client) {
    this.client = client;
  }

}
