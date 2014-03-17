package com.inmobi.grill.server;

import java.io.IOException;

import javax.ws.rs.core.UriBuilder;

import org.apache.hadoop.hive.conf.HiveConf;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;

import com.inmobi.grill.server.api.GrillConfConstants;

public class EmbeddedServer {
  final HttpServer server;
  final HiveConf conf;
  
  EmbeddedServer(HiveConf conf) throws IOException {
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

  public void stop() {
    server.shutdownNow();
    GrillServices.get().stop();
  }

  public static void main(String[] args) throws Exception {
    EmbeddedServer thisServer = new EmbeddedServer(new HiveConf());
    System.in.read();
    thisServer.stop();
  }

}
