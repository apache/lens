package com.inmobi.grill.server;

import java.io.IOException;

import javax.ws.rs.core.UriBuilder;

import org.apache.hadoop.hive.conf.HiveConf;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;

import com.inmobi.grill.server.api.GrillConfConstants;

public class GrillServer {
  final HttpServer server;
  final HiveConf conf;
  
  GrillServer(HiveConf conf) throws IOException {
    this.conf = conf;
    startServices(conf);
    String baseURI = conf.get(GrillConfConstants.GRILL_SERVER_BASE_URL,
        GrillConfConstants.DEFAULT_GRILL_SERVER_BASE_URL);
    server = GrizzlyHttpServerFactory.createHttpServer(UriBuilder.fromUri(baseURI).build(),
        ResourceConfig.forApplicationClass(AllApps.class));
  }

  public void startServices(HiveConf conf) {
    GrillServices.get().init(conf);
    GrillServices.get().start();
  }

  public void start() throws IOException {
    server.start();
  }

  public void stop() {
    server.shutdownNow();
    GrillServices.get().stop();
  }

  public static void main(String[] args) throws Exception {
    GrillServer thisServer = new GrillServer(new HiveConf());
    Runtime.getRuntime().addShutdownHook(
        new Thread(new ServerShutdownHook(thisServer), "Shutdown Thread"));
    thisServer.start();
    System.in.read();
  }

  public static class ServerShutdownHook implements Runnable {

    private final GrillServer thisServer;

    public ServerShutdownHook(GrillServer server) {
      this.thisServer = server;
    }

    @Override
    public void run() {
      try {
        // Stop the Composite Service
        thisServer.stop();
      } catch (Throwable t) {
      }
    }
  }

}
