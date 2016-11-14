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
package org.apache.lens.server.api.driver.hooks;

import java.util.List;

import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.QueryContext;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;
import lombok.Data;

/**
 * This hook allows chaining of different hooks. A driver can specify multiple hooks for itself by providing
 * `query.hook.classes` in its configuration. The value should be a comma separated list of class names, each
 * of which should be an implementation of org.apache.lens.server.api.driver.hooks.DriverQueryHook. This hook
 * stores instances of those classes.
 * All the methods of this class invokes the same method on each of the hooks in the chain.
 */
@Data
public class ChainedDriverQueryHook extends NoOpDriverQueryHook {

  private final Iterable<DriverQueryHook> hooks;

  @Override
  public void setDriver(LensDriver driver) {
    super.setDriver(driver);
    for (DriverQueryHook hook : hooks) {
      hook.setDriver(driver);
    }
  }

  @Override
  public void preRewrite(AbstractQueryContext ctx) throws LensException {
    super.preRewrite(ctx);
    for (DriverQueryHook hook : hooks) {
      hook.preRewrite(ctx);
    }
  }

  @Override
  public void postRewrite(AbstractQueryContext ctx) throws LensException {
    super.postRewrite(ctx);
    for (DriverQueryHook hook : hooks) {
      hook.postRewrite(ctx);
    }
  }

  @Override
  public void preEstimate(AbstractQueryContext ctx) throws LensException {
    super.preEstimate(ctx);
    for (DriverQueryHook hook : hooks) {
      hook.preEstimate(ctx);
    }
  }

  @Override
  public void postEstimate(AbstractQueryContext ctx) throws LensException {
    super.postEstimate(ctx);
    for (DriverQueryHook hook : hooks) {
      hook.postEstimate(ctx);
    }
  }

  @Override
  public void postDriverSelection(AbstractQueryContext ctx) throws LensException {
    super.postDriverSelection(ctx);
    for (DriverQueryHook hook : hooks) {
      hook.postDriverSelection(ctx);
    }
  }

  @Override
  public void preLaunch(QueryContext ctx) throws LensException {
    super.preLaunch(ctx);
    for (DriverQueryHook hook : hooks) {
      hook.preLaunch(ctx);
    }
  }

  public static ChainedDriverQueryHook from(Configuration conf, String key) throws LensException {
    String[] classNames = conf.getStrings(key);
    List<DriverQueryHook> hooks = Lists.newArrayList();
    if (classNames != null) {
      for (String className : classNames) {
        Class<? extends DriverQueryHook> clazz;
        try {
          clazz = conf.getClassByName(className).asSubclass(DriverQueryHook.class);
        } catch (ClassNotFoundException e) {
          throw new LensException("Couldn't load class " + className, e);
        }
        DriverQueryHook instance;
        try {
          instance = clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
          throw new LensException("Couldn't create instance of class " + clazz.getName(), e);
        }
        if (instance instanceof Configurable) {
          ((Configurable) instance).setConf(conf);
        }
        hooks.add(instance);
      }
    }
    return new ChainedDriverQueryHook(hooks);
  }
}
