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
package org.apache.lens.server.api.authorization;

import org.apache.lens.server.api.LensConfConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

//Singleton instance of Authorizer class
public final class LensAuthorizer {

  private static final LensAuthorizer INSTANCE = new LensAuthorizer();

  private Authorizer authorizer;

  // private constructor to ensure single instance.
  private LensAuthorizer() {
  }

  public void init(Configuration hiveConf){
    this.authorizer =  ReflectionUtils.newInstance(
    hiveConf.getClass(LensConfConstants.AUTHORIZER_CLASS, LensConfConstants.DEFAULT_AUTHORIZER, Authorizer.class),
    hiveConf);
  }

  /**
   *
   * @return the singleton instance of the authorizer.
   */
  public static LensAuthorizer get(){
    return INSTANCE;
  }

  public Authorizer getAuthorizer() {
    return this.authorizer;
  }


}
