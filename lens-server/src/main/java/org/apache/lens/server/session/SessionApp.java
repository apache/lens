package org.apache.lens.server.session;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import org.apache.lens.server.LensApplicationListener;
import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

/**
 * The Class SessionApp.
 */
@ApplicationPath("/session")
public class SessionApp extends Application {
  @Override
  public Set<Class<?>> getClasses() {
    final Set<Class<?>> classes = new HashSet<Class<?>>();
    // register root resource
    classes.add(SessionResource.class);
    classes.add(MultiPartFeature.class);
    classes.add(LoggingFilter.class);
    classes.add(LensApplicationListener.class);
    return classes;
  }

}
