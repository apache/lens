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

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * The Class LensServer.
 */
public class LensServer {

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(LensServer.class);

  private static final String SEP_LINE =
    "\n###############################################################\n";

  /** The server. */
  final HttpServer server;

  /** The ui server. */
  final HttpServer uiServer;

  /** The conf. */
  final HiveConf conf;

  /** This flag indicates that the lens server can run,
   * When this is set to false, main thread bails out.
   */
  volatile boolean canRun = true;

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
  public synchronized void start() throws IOException {
    server.start();
    uiServer.start();
  }

  /**
   * Stop.
   */
  public synchronized void stop() {
    server.shutdownNow();
    uiServer.shutdownNow();
    LensServices.get().stop();
    printShutdownMessage();
  }

  /**
   * This keeps the server running till a shutdown is
   * triggered. Either through a shutdown sequence initiated
   * by an administrator or if applications encounters a
   * fatal exception or it enters an unrecoverable state.
   */
  private void join() {
    while (canRun) {
      synchronized (this) {
        try {
          wait(2000);
        } catch (InterruptedException e) {
          LOG.warn("Received an interrupt in the main loop", e);
        }
      }
    }
    LOG.info("Exiting main run loop...");
  }

  /**
   * The main method.
   *
   * @param args
   *          the arguments
   * @throws Exception
   *           the exception
   */
  public static void main(String[] args) throws Exception {

    printStartupMessage();
    try {
      final LensServer thisServer = new LensServer(LensServerConf.get());

      registerShutdownHook(thisServer);
      registerDefaultExceptionHandler();

      thisServer.start();
      thisServer.join();
    } catch (Exception exc) {
      LOG.fatal("Error while creating Lens server", exc);
      try {
        LensServices.get().stop();
      } catch (Exception e) {
        LOG.error("Error stopping services", e);
      }
    }
  }

  /**
   * Print message from lens-build-info file during startup.
   */
  private static void printStartupMessage() {
    StringBuilder buffer = new StringBuilder();
    buffer.append(SEP_LINE);
    buffer.append("                    Lens Server (STARTUP)");
    Properties buildProperties = new Properties();
    InputStream buildPropertiesResource = LensServer.class.
      getResourceAsStream("/lens-build-info.properties");
    if (buildPropertiesResource != null) {
      try {
        buildProperties.load(buildPropertiesResource);
        for (Map.Entry entry : buildProperties.entrySet()) {
              buffer.append('\n').append('\t').append(entry.getKey()).
                      append(":\t").append(entry.getValue());
          }
      } catch (Throwable e) {
          buffer.append("*** Unable to get build info ***");
      }
    } else {
      buffer.append("*** Unable to get build info ***");
    }
    buffer.append(SEP_LINE);
    LOG.info(buffer.toString());
  }

  /**
   * Print message before the lens server stops.
   */
  private static void printShutdownMessage() {
    StringBuilder buffer = new StringBuilder();
    buffer.append(SEP_LINE);
    buffer.append("                    Lens Server (SHUTDOWN)");
    buffer.append(SEP_LINE);
    LOG.info(buffer.toString());
  }

  /** Registering a shutdown hook to listen to SIGTERM events.
   * Upon receiving a SIGTERM, notify the server, which is put
   * on wait state.
   */
  private static void registerShutdownHook(final LensServer thisServer) {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        Thread.currentThread().setName("Shutdown");
        LOG.info("Server has been requested to be stopped.");
        thisServer.canRun = false;
        thisServer.stop();
      }
    });
  }

  /** Registering a default uncaught exception handler. */
  private static void registerDefaultExceptionHandler() {
    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        LOG.fatal("Uncaught exception in Thread " + t, e);
      }
    });
  }
}
