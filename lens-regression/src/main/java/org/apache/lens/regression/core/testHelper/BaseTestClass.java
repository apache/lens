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

import org.apache.lens.regression.core.helpers.LensHelper;
import org.apache.lens.regression.core.helpers.LensServerHelper;
import org.apache.lens.regression.core.helpers.MetastoreHelper;
import org.apache.lens.regression.core.helpers.QueryHelper;
import org.apache.lens.regression.core.helpers.SessionHelper;

public class BaseTestClass {

  private LensHelper lensHelper;
  private QueryHelper qHelper;
  private MetastoreHelper mHelper;
  private SessionHelper sHelper;
  private LensServerHelper lens;

  public static final String LENS_PROPERTIES = "lens.properties";

  public BaseTestClass() {

    lensHelper = new LensHelper(LENS_PROPERTIES);
    qHelper = lensHelper.getQueryHelper();
    mHelper = lensHelper.getMetastoreHelper();
    sHelper = lensHelper.getSessionHelper();
    lens = lensHelper.getServerHelper();
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


}
