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

package org.apache.lens.regression.core.helpers;

public class LensHelper {

  protected QueryHelper queryHelper;
  protected MetastoreHelper metastoreHelper;
  protected SessionHelper sessionHelper;
  protected LensServerHelper serverHelper;
  protected String envFileName;

  public LensHelper(String envFileName) {
    this.envFileName = envFileName;
    queryHelper = new QueryHelper(envFileName);
    metastoreHelper = new MetastoreHelper(envFileName);
    sessionHelper = new SessionHelper(envFileName);
    serverHelper = new LensServerHelper(envFileName);
  }

  public QueryHelper getQueryHelper() {
    return queryHelper;
  }

  public MetastoreHelper getMetastoreHelper() {
    return metastoreHelper;
  }

  public SessionHelper getSessionHelper() {
    return sessionHelper;
  }

  public LensServerHelper getServerHelper() {
    return serverHelper;
  }

  public String getEnvFileName() {
    return envFileName;
  }

}
