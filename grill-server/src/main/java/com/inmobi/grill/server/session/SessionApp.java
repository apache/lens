package com.inmobi.grill.server.session;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.media.multipart.MultiPartFeature;


@ApplicationPath("/session")
public class SessionApp extends Application {
  @Override
  public Set<Class<?>> getClasses() {
      final Set<Class<?>> classes = new HashSet<Class<?>>();
      // register root resource
      classes.add(SessionResource.class);
      classes.add(MultiPartFeature.class);
      classes.add(LoggingFilter.class);
      return classes;
  }

}
