package com.inmobi.grill.server;

/*
 * #%L
 * Grill Server
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.codahale.metrics.servlets.AdminServlet;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.ui.UIApp;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
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

public class GrillServer {
  public static final Log LOG = LogFactory.getLog(GrillServer.class);

  final HttpServer server;
  final HttpServer uiServer;
  final HiveConf conf;

  static {
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }

  private GrillServer(HiveConf conf) throws IOException {
    this.conf = conf;
    startServices(conf);
    String baseURI = conf.get(GrillConfConstants.GRILL_SERVER_BASE_URL,
        GrillConfConstants.DEFAULT_GRILL_SERVER_BASE_URL);
    server = GrizzlyHttpServerFactory.createHttpServer(UriBuilder.fromUri(baseURI).build(),
        getApp(), false);

    WebappContext adminCtx = new WebappContext("admin", "");
    adminCtx.setAttribute("com.codahale.metrics.servlets.MetricsServlet.registry",
        ((MetricsServiceImpl)GrillServices.get().getService(MetricsServiceImpl.METRICS_SVC_NAME))
        .getMetricRegistry());
    adminCtx.setAttribute("com.codahale.metrics.servlets.HealthCheckServlet.registry",
        ((MetricsServiceImpl)GrillServices.get().getService(MetricsServiceImpl.METRICS_SVC_NAME))
        .getHealthCheck());

    final ServletRegistration sgMetrics = adminCtx.addServlet("admin", new AdminServlet());
    sgMetrics.addMapping("/admin/*");

    adminCtx.deploy(this.server);
    String uiServerURI = conf.get(GrillConfConstants.GRILL_SERVER_UI_URI,
      GrillConfConstants.DEFAULT_GRILL_SERVER_UI_URI);
    this.uiServer = GrizzlyHttpServerFactory.createHttpServer(UriBuilder.fromUri(uiServerURI).build(),
      getUIApp(), false);
  }

  private ResourceConfig getApp() {
    ResourceConfig app = ResourceConfig.forApplicationClass(AllApps.class);
    app.register(new LoggingFilter(Logger.getLogger(GrillServer.class.getName() + ".request"), true));
    app.setApplicationName("AllApps");
    return app;
  }

  private ResourceConfig getUIApp() {
    ResourceConfig uiApp = ResourceConfig.forApplicationClass(UIApp.class);
    uiApp.register(
      new LoggingFilter(Logger.getLogger(GrillServer.class.getName() + ".ui_request"), true));
    uiApp.setApplicationName("GrillUI");
    return uiApp;
  }

  public void startServices(HiveConf conf) {
    GrillServices.get().init(conf);
    GrillServices.get().start();
  }

  public void start() throws IOException {
    server.start();
    uiServer.start();
  }

  public void stop() {
    server.shutdownNow();
    uiServer.shutdownNow();
    GrillServices.get().stop();
  }

  private static GrillServer thisServer;

  @SuppressWarnings("restriction")
  public static void main(String[] args) throws Exception {
    Signal.handle(new Signal("TERM"), new SignalHandler() {

      @Override
      public void handle(Signal signal) {
        try {
          LOG.info("Request for stopping grill server received");
          if (thisServer != null) {
            synchronized (thisServer) {
              thisServer.notify();
            }
          }
        }
        catch (Exception e) {
          LOG.warn("Error in shutting down databus", e);
        }
      }
    });

    try {
      thisServer = new GrillServer(new HiveConf());
    } catch (Exception exc) {
      LOG.fatal("Error while creating Grill server", exc);
      try {
        GrillServices.get().stop();
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
