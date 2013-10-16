package com.inmobi.grill.service;

/**
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.webapp.WebAppContext;
*/

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletContainer;
import org.glassfish.jersey.grizzly2.servlet.GrizzlyWebContainerFactory;

public class EmbeddedServer {
  private static final String APP_PATH = "app";
  private static final String APP_PORT = "port";
  
  private final HttpServer server;;

  public EmbeddedServer(int port, String path) {
    server = HttpServer.createSimpleServer(null, "localhost", port);
  }

  public EmbeddedServer() {
    server = HttpServer.createSimpleServer();
  }

  public void start() throws Exception {
     // Services.get().reset();
      server.start();
  }

  public void stop() throws Exception {
      server.shutdownNow();
  }

  private static CommandLine parseArgs(String[] args) throws ParseException {
    Options options = new Options();
    Option opt;

    opt = new Option(APP_PATH, true, "Application Path");
    opt.setRequired(false);
    options.addOption(opt);

    opt = new Option(APP_PORT, true, "Application Port");
    opt.setRequired(false);
    options.addOption(opt);

    return new GnuParser().parse(options, args);
}

  public static void main(String[] args) throws Exception {
 /*   CommandLine cmd = parseArgs(args);
    //String projectVersion = System.getProperty("project.version");
    String appPath = "/api/";//"target/grill-server-0.1-SNAPSHOT"; //+ projectVersion;
    int appPort = 20000;

    if (cmd.hasOption(APP_PATH)) {
        appPath = cmd.getOptionValue(APP_PATH);
    }

    if (cmd.hasOption(APP_PORT)) {
        appPort = Integer.valueOf(cmd.getOptionValue(APP_PORT));
    }

    EmbeddedServer server = new EmbeddedServer(appPort, appPath);
    server.start();*/
    URI BASE_URI = URI.create("http://localhost:8080/");
    String ROOT_PATH = "queryapi";
    Map<String, String> initParams = new HashMap<String, String>();
    initParams.put(
            ServerProperties.PROVIDER_PACKAGES,
            IndexResource.class.getPackage().getName());
    final HttpServer server = GrizzlyWebContainerFactory.create(BASE_URI, ServletContainer.class, initParams);
    System.out.println(String.format("Application started.%nTry out %s%s%nHit enter to stop it...",
        BASE_URI, ROOT_PATH));
    System.in.read();
    server.shutdownNow();
  }

}
