package org.apache.lens.server;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

//import org.glassfish.jersey.server.ResourceConfig;

import java.util.HashSet;
import java.util.Set;

/**
 * The Class IndexApp.
 */
@ApplicationPath("/")
public class IndexApp extends Application {

  @Override
  public Set<Class<?>> getClasses() {
    final Set<Class<?>> classes = new HashSet<Class<?>>();
    // register root resource
    classes.add(IndexResource.class);
    return classes;
  }
}
