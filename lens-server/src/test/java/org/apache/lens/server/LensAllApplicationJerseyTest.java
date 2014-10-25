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
package org.apache.lens.server;

import javax.ws.rs.core.Application;

import org.apache.lens.server.LensApplication;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

/**
 * The Class LensAllApplicationJerseyTest.
 */
public abstract class LensAllApplicationJerseyTest extends LensJerseyTest {

  /*
   * (non-Javadoc)
   * 
   * @see org.glassfish.jersey.test.JerseyTest#configure()
   */
  @Override
  protected Application configure() {
    return new LensApplication();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.glassfish.jersey.test.JerseyTest#configureClient(org.glassfish.jersey.client.ClientConfig)
   */
  @Override
  protected void configureClient(ClientConfig config) {
    config.register(MultiPartFeature.class);
  }

}
