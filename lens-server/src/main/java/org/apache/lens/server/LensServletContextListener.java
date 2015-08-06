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

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.CompositeService;

/**
 * Initialize the webapp.
 */
public class LensServletContextListener implements ServletContextListener {

  /**
   * * Notification that the web application initialization * process is starting. * All ServletContextListeners are
   * notified of context * initialization before any filter or servlet in the web * application is initialized.
   *
   * @param sce the sce
   */
  @Override
  public void contextInitialized(ServletContextEvent sce) {
    // start up all lens services
    HiveConf conf = LensServerConf.getHiveConf();
    LensServices services = LensServices.get();
    services.init(conf);
    services.start();

    // initialize hiveConf for WS resources
    Runtime.getRuntime().addShutdownHook(new Thread(new CompositeService.CompositeServiceShutdownHook(services)));
  }

  /**
   * * Notification that the servlet context is about to be shut down. * All servlets and filters have been destroy()ed
   * before any * ServletContextListeners are notified of context * destruction.
   *
   * @param sce the sce
   */
  @Override
  public void contextDestroyed(ServletContextEvent sce) {
    LensServices.get().stop();
  }
}
