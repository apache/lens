package com.inmobi.grill.service;

import java.util.List;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * Initialize the webapp
 */
public class GrillServletContextListener  implements ServletContextListener {
  public static final String LOG_PROPERTIES_FILE_KEY = "grill.server.log4j.properties";

  /**
   * * Notification that the web application initialization
   * * process is starting.
   * * All ServletContextListeners are notified of context
   * * initialization before any filter or servlet in the web
   * * application is initialized.
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

    // start up all grill services
    GrillServices services = GrillServices.get();
    services.initServices();
  }

  /**
   * * Notification that the servlet context is about to be shut down.
   * * All servlets and filters have been destroy()ed before any
   * * ServletContextListeners are notified of context
   * * destruction.
   */
  @Override
  public void contextDestroyed(ServletContextEvent sce) {
    GrillServices.get().stopAll();
  }
}
