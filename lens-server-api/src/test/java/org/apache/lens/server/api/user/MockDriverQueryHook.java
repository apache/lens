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
package org.apache.lens.server.api.user;

import java.util.HashMap;

import org.apache.lens.server.api.driver.NoOpDriverQueryHook;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.QueryContext;

public class MockDriverQueryHook extends NoOpDriverQueryHook {
  public static final String KEY_PRE_LAUNCH = "TEST_KEY_PRE_LAUNCH";
  public static final String VALUE_PRE_LAUNCH = "TEST_VALUE_PRE_LAUNCH";

  public static final String KEY_POST_SELECT = "TEST_KEY_POST_SELECT";
  public static final String VALUE_POST_SELECT = "TEST_VALUE_POST_SELECT";
  public static final String UNSAVED_KEY_POST_SELECT = "TEST_UNSAVED__KEY_POST_SELECT";
  public static final String UNSAVED_VALUE_POST_SELECT = "TEST_UNSAVED_VALUE_POST_SELECT";

  public static final String PRE_REWRITE = "PRE_REWRITE";
  public static final String POST_REWRITE = "POST_REWRITE";
  public static final String PRE_ESTIMATE = "PRE_ESTIMATE";
  public static final String POST_ESTIMATE = "POST_ESTIMATE";

  @Override
  public void preLaunch(QueryContext ctx) {
    super.preLaunch(ctx);
    ctx.getSelectedDriverConf().set(KEY_PRE_LAUNCH, VALUE_PRE_LAUNCH);
  }

  @Override
  public void postDriverSelection(AbstractQueryContext ctx) throws LensException {
    super.postDriverSelection(ctx);

    //Updated both in driver config and LensConf(which gets persisted)
    ctx.getSelectedDriverConf().set(KEY_POST_SELECT, VALUE_POST_SELECT);
    ctx.updateConf(new HashMap<String, String>(1) {{
        put(KEY_POST_SELECT, VALUE_POST_SELECT);
      }
    });

    //Updated only in driver conf.
    ctx.getSelectedDriverConf().set(UNSAVED_KEY_POST_SELECT, UNSAVED_VALUE_POST_SELECT);
  }

  @Override
  public void preRewrite(AbstractQueryContext ctx) throws LensException {
    super.preRewrite(ctx);
    ctx.getDriverConf(getDriver()).set(PRE_REWRITE, PRE_REWRITE);
  }

  @Override
  public void postRewrite(AbstractQueryContext ctx) throws LensException {
    super.postRewrite(ctx);
    ctx.getDriverConf(getDriver()).set(POST_REWRITE, POST_REWRITE);
  }

  @Override
  public void preEstimate(AbstractQueryContext ctx) throws LensException {
    super.preEstimate(ctx);
    ctx.getDriverConf(getDriver()).set(PRE_ESTIMATE, PRE_ESTIMATE);
  }

  @Override
  public void postEstimate(AbstractQueryContext ctx) throws LensException {
    super.postEstimate(ctx);
    ctx.getDriverConf(getDriver()).set(POST_ESTIMATE, POST_ESTIMATE);
  }
}
