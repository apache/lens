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

package org.apache.lens.regression.core.testHelper;

import org.apache.lens.regression.core.helpers.*;

public class BaseTestClass {

  protected QueryHelper qHelper;
  protected MetastoreHelper mHelper;
  protected SessionHelper sHelper;
  protected LensServerHelper lens;
  protected SavedQueryResourceHelper savedQueryResourceHelper;
  protected ScheduleResourceHelper scheduleHelper;

  public static final String LENS_PROPERTIES = "lens.properties";

  public BaseTestClass() {
    qHelper = new QueryHelper(LENS_PROPERTIES);
    mHelper = new MetastoreHelper(LENS_PROPERTIES);
    sHelper = new SessionHelper(LENS_PROPERTIES);
    lens = new LensServerHelper(LENS_PROPERTIES);
    savedQueryResourceHelper = new SavedQueryResourceHelper(LENS_PROPERTIES);
    scheduleHelper = new ScheduleResourceHelper(LENS_PROPERTIES);
  }

  public QueryHelper getQueryHelper() {
    return qHelper;
  }

  public MetastoreHelper getMetastoreHelper() {
    return mHelper;
  }

  public SessionHelper getSessionHelper() {
    return sHelper;
  }

  public LensServerHelper getLensServerHelper() {
    return lens;
  }

  public SavedQueryResourceHelper getSavedQueryResourceHelper() {
    return savedQueryResourceHelper;
  }


}
