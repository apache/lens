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
package org.apache.lens.client;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.lens.api.LensConf;

/**
 * Top level class which encapsulates connections parameters required for lens connection.
 */
public class LensConnectionParams {

  /** The lens confs. */
  private Map<String, String> lensConfs = new HashMap<String, String>();

  /** The lens vars. */
  private Map<String, String> lensVars = new HashMap<String, String>();

  /** The session vars. */
  private Map<String, String> sessionVars = new HashMap<String, String>();

  /** The conf. */
  private final LensClientConfig conf;

  /**
   * Construct parameters required to connect to lens server using values in lens-client-site.xml
   */
  public LensConnectionParams() {
    this.conf = new LensClientConfig();
  }

  /**
   * Construct parameters required to connect to lens server from values passed in configuration.
   *
   * @param conf
   *          from which connection parameters are defined.
   */
  public LensConnectionParams(LensClientConfig conf) {
    this.conf = conf;
  }

  /**
   * Gets the Database to which lens client should connect to.
   *
   * @return database to connect to
   */
  public String getDbName() {
    return conf.getLensDatabase();
  }

  public Map<String, String> getLensConfs() {
    return lensConfs;
  }

  public Map<String, String> getLensVars() {
    return lensVars;
  }

  public Map<String, String> getSessionVars() {
    return sessionVars;
  }

  public void setDbName(String dbName) {
    this.conf.setLensDatabase(dbName);
  }

  public String getBaseConnectionUrl() {
    return conf.getBaseURL();
  }

  public LensClientConfig getConf() {
    return this.conf;
  }

  public String getUser() {
    return this.conf.getUser();
  }

  public String getSessionResourcePath() {
    return this.conf.getSessionResourcePath();
  }

  public String getQueryResourcePath() {
    return this.conf.getQueryResourcePath();
  }

  public String getMetastoreResourcePath() {
    return this.conf.getMetastoreResourcePath();
  }

  public long getQueryPollInterval() {
    return this.conf.getQueryPollInterval();
  }

  public LensConf getSessionConf() {
    LensConf conf = new LensConf();
    Iterator<Map.Entry<String, String>> itr = this.conf.iterator();
    while (itr.hasNext()) {
      Map.Entry<String, String> entry = itr.next();
      conf.addProperty(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, String> entry : lensConfs.entrySet()) {
      conf.addProperty(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, String> entry : sessionVars.entrySet()) {
      conf.addProperty(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, String> entry : lensVars.entrySet()) {
      conf.addProperty(entry.getKey(), entry.getValue());
    }
    return conf;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return new StringBuilder("LensConnectionParams{").append("dbName='").append(this.conf.getLensDatabase())
        .append('\'').append(", baseUrl='").append(this.conf.getBaseURL()).append('\'').append(", user=")
        .append(this.conf.getUser()).append(", lensConfs=").append(lensConfs).append(", lensVars=").append(lensVars)
        .append(", sessionVars=").append(sessionVars).append('}').toString();
  }

  public void setBaseUrl(String baseUrl) {
    conf.setBaseUrl(baseUrl);
  }
}
