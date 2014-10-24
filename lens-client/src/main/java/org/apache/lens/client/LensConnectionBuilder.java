package org.apache.lens.client;

/*
 * #%L
 * Lens client
 * %%
 * Copyright (C) 2014 Apache Software Foundation
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
public class LensConnectionBuilder {

  private String baseUrl;
  private String database;
  private String user;
  private String password;

  public LensConnectionBuilder() {
  }

  public LensConnectionBuilder baseUrl(String baseUrl) {
    this.baseUrl = baseUrl;
    return this;
  }

  public LensConnectionBuilder database(String database) {
    this.database = database;
    return this;
  }

  public LensConnectionBuilder user(String user) {
    this.user = user;
    return this;
  }

  public LensConnectionBuilder password(String password) {
    this.password = password;
    return this;
  }


  public LensConnection build() {
    LensConnectionParams params = new LensConnectionParams();
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
    return new LensConnection(params);
  }

}
