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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.CompositeService;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * Initialize the webapp.
 *
 * @see LensServletContextEvent
 */
public class LensServletContextListener implements ServletContextListener {

  /** The Constant LOG_PROPERTIES_FILE_KEY. */
  public static final String LOG_PROPERTIES_FILE_KEY = "lens.server.log4j.properties";

  /**
   * * Notification that the web application initialization * process is starting. * All ServletContextListeners are
   * notified of context * initialization before any filter or servlet in the web * application is initialized.
   *
   * @param sce
   *          the sce
   */
  @Override
  public void contextInitialized(ServletContextEvent sce) {
    // Initialize logging
    try {
      String log4jPropertyFile = sce.getServletContext().getInitParameter(LOG_PROPERTIES_FILE_KEY);
      if (log4jPropertyFile != null && !log4jPropertyFile.isEmpty()) {
        String basePath = sce.getServletContext().getRealPath("/");
        System.out.println("Application BasePath:" + basePath);
        PropertyConfigurator.configure(basePath + "/" + log4jPropertyFile);
      } else {
        System.err.println("WARN - Empty value for " + LOG_PROPERTIES_FILE_KEY + ", using BasicConfigurator");
        BasicConfigurator.configure();
      }
    } catch (Exception exc) {
      // Try basic configuration
      System.err.println("WARNING - log4j property configurator gave error, falling back to basic configurator");
      exc.printStackTrace();
      BasicConfigurator.configure();
    }

    // start up all lens services
    HiveConf conf = LensServerConf.get();
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
   * @param sce
   *          the sce
   */
  @Override
  public void contextDestroyed(ServletContextEvent sce) {
    LensServices.get().stop();
  }
}
