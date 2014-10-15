package org.apache.lens.client;

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

/**
 * Top level builder class for lens connection
 */
public class GrillConnectionBuilder {

  private String baseUrl;
  private String database;
  private String user;
  private String password;

  public GrillConnectionBuilder() {
  }

  public GrillConnectionBuilder baseUrl(String baseUrl) {
    this.baseUrl = baseUrl;
    return this;
  }

  public GrillConnectionBuilder database(String database) {
    this.database = database;
    return this;
  }

  public GrillConnectionBuilder user(String user) {
    this.user = user;
    return this;
  }

  public GrillConnectionBuilder password(String password) {
    this.password = password;
    return this;
  }


  public GrillConnection build() {
    GrillConnectionParams params = new GrillConnectionParams();
    if(baseUrl != null && !baseUrl.isEmpty()){
      params.setBaseUrl(baseUrl);
    }
    if(database != null && !database.isEmpty()) {
      params.setDbName(database);
    }
    if (user != null && !user.isEmpty()) {
      params.getSessionVars().put("user.name", user);
    }
    if (password != null && !password.isEmpty()) {
      params.getSessionVars().put("user.pass", password);
    }
    return new GrillConnection(params);
  }

}
