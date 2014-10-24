package org.apache.lens.server.quota;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import org.apache.lens.server.LensApplicationListener;
import org.glassfish.jersey.filter.LoggingFilter;

/**
 * The Class QuotaApp.
 */
@ApplicationPath("/quota")
public class QuotaApp extends Application {
  @Override
  public Set<Class<?>> getClasses() {
    final Set<Class<?>> classes = new HashSet<Class<?>>();
    // register root resource
    classes.add(QuotaResource.class);
    classes.add(LensApplicationListener.class);
    classes.add(LoggingFilter.class);
    return classes;
  }
}
