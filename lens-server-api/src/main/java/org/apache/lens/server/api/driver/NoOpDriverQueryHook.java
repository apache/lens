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
/*
 *
 */
package org.apache.lens.server.api.driver;

import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.QueryContext;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NoOpDriverQueryHook implements DriverQueryHook {

  @Getter
  private LensDriver driver;

  @Override
  public void setDriver(LensDriver driver) {
    this.driver = driver;
    log.debug("The Driver for this driver query hook is {}", driver.getFullyQualifiedName());
  }

  @Override
  public void preRewrite(AbstractQueryContext ctx) throws LensException {
    log.debug("Pre rewrite for user {}, user query: {}, driver: {}", ctx.getSubmittedUser(), ctx.getUserQuery(),
      driver.getFullyQualifiedName());
  }

  @Override
  public void postRewrite(AbstractQueryContext ctx) throws LensException {
    log.debug("Post rewrite for user {}, user query: {}, driver: {}, driver query :{}", ctx.getSubmittedUser(),
      ctx.getUserQuery(), driver.getFullyQualifiedName(), ctx.getDriverQuery(driver));
  }

  @Override
  public void preEstimate(AbstractQueryContext ctx) throws LensException {
    log.debug("Pre estimate for user {}, user query: {}, driver: {}, driver query :{}", ctx.getSubmittedUser(),
      ctx.getUserQuery(), driver.getFullyQualifiedName(), ctx.getDriverQuery(driver));
  }

  @Override
  public void postEstimate(AbstractQueryContext ctx) throws LensException {
    log.debug("Post estimate for user {}, user query: {}, driver: {}, driver query :{},  query cost :{}",
      ctx.getSubmittedUser(), ctx.getUserQuery(), driver.getFullyQualifiedName(), ctx.getDriverQuery(driver),
      ctx.getDriverQueryCost(driver));
  }

  @Override
  public void postDriverSelection(AbstractQueryContext ctx) throws LensException {
    log.debug("Post driver selection for user {}, user query: {}, driver {}, driver query: {}", ctx.getSubmittedUser(),
      ctx.getUserQuery(), ctx.getSelectedDriver().getFullyQualifiedName(), ctx.getSelectedDriverQuery());
  }

  @Override
  public void preLaunch(QueryContext ctx) {
    log.debug("Pre launch for user {}, user query: {}, driver {}, driver query: {}", ctx.getSubmittedUser(),
      ctx.getUserQuery(), ctx.getSelectedDriver().getFullyQualifiedName(), ctx.getSelectedDriverQuery());
  }
}
