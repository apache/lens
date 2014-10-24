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

import com.codahale.metrics.servlets.AdminServlet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.ui.UIApp;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.servlet.ServletRegistration;
import org.glassfish.grizzly.servlet.WebappContext;
import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.bridge.SLF4JBridgeHandler;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * The Class LensServer.
 */
public class LensServer {

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(LensServer.class);

  /** The server. */
  final HttpServer server;

  /** The ui server. */
  final HttpServer uiServer;

  /** The conf. */
  final HiveConf conf;

  static {
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }

  /**
   * Instantiates a new lens server.
   *
   * @param conf
   *          the conf
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private LensServer(HiveConf conf) throws IOException {
    this.conf = conf;
    startServices(conf);
    String baseURI = conf.get(LensConfConstants.SERVER_BASE_URL, LensConfConstants.DEFAULT_SERVER_BASE_URL);
    server = GrizzlyHttpServerFactory.createHttpServer(UriBuilder.fromUri(baseURI).build(), getApp(), false);

    WebappContext adminCtx = new WebappContext("admin", "");
    adminCtx.setAttribute("com.codahale.metrics.servlets.MetricsServlet.registry", ((MetricsServiceImpl) LensServices
        .get().getService(MetricsServiceImpl.METRICS_SVC_NAME)).getMetricRegistry());
    adminCtx.setAttribute("com.codahale.metrics.servlets.HealthCheckServlet.registry",
        ((MetricsServiceImpl) LensServices.get().getService(MetricsServiceImpl.METRICS_SVC_NAME)).getHealthCheck());

    final ServletRegistration sgMetrics = adminCtx.addServlet("admin", new AdminServlet());
    sgMetrics.addMapping("/admin/*");

    adminCtx.deploy(this.server);
    String uiServerURI = conf.get(LensConfConstants.SERVER_UI_URI, LensConfConstants.DEFAULT_SERVER_UI_URI);
    this.uiServer = GrizzlyHttpServerFactory.createHttpServer(UriBuilder.fromUri(uiServerURI).build(), getUIApp(),
        false);
  }

  private ResourceConfig getApp() {
    ResourceConfig app = ResourceConfig.forApplicationClass(LensApplication.class);
    app.register(new LoggingFilter(Logger.getLogger(LensServer.class.getName() + ".request"), true));
    app.setApplicationName("AllApps");
    return app;
  }

  private ResourceConfig getUIApp() {
    ResourceConfig uiApp = ResourceConfig.forApplicationClass(UIApp.class);
    uiApp.register(new LoggingFilter(Logger.getLogger(LensServer.class.getName() + ".ui_request"), true));
    uiApp.setApplicationName("Lens UI");
    return uiApp;
  }

  /**
   * Start services.
   *
   * @param conf
   *          the conf
   */
  public void startServices(HiveConf conf) {
    LensServices.get().init(conf);
    LensServices.get().start();
  }

  /**
   * Start.
   *
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void start() throws IOException {
    server.start();
    uiServer.start();
  }

  /**
   * Stop.
   */
  public void stop() {
    server.shutdownNow();
    uiServer.shutdownNow();
    LensServices.get().stop();
  }

  /** The this server. */
  private static LensServer thisServer;

  /**
   * The main method.
   *
   * @param args
   *          the arguments
   * @throws Exception
   *           the exception
   */
  @SuppressWarnings("restriction")
  public static void main(String[] args) throws Exception {
    Signal.handle(new Signal("TERM"), new SignalHandler() {

      @Override
      public void handle(Signal signal) {
        try {
          LOG.info("Request for stopping lens server received");
          if (thisServer != null) {
            synchronized (thisServer) {
              thisServer.notify();
            }
          }
        } catch (Exception e) {
          LOG.warn("Error in shutting down databus", e);
        }
      }
    });

    try {
      thisServer = new LensServer(LensServerConf.get());
    } catch (Exception exc) {
      LOG.fatal("Error while creating Lens server", exc);
      try {
        LensServices.get().stop();
      } catch (Exception e) {
        LOG.error("Error stopping services", e);
      }
      System.exit(1);
    }

    thisServer.start();
    synchronized (thisServer) {
      thisServer.wait();
    }
    thisServer.stop();
    System.exit(0);
  }
}
