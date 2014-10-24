package org.apache.lens.server.metastore;


import org.apache.lens.server.LensApplicationListener;
import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.media.multipart.MultiPartFeature;


import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

@ApplicationPath("/")
public class MetastoreApp extends Application {
  @Override
  public Set<Class<?>> getClasses() {
    final Set<Class<?>> classes = new HashSet<Class<?>>();
    // register root resource
    classes.add(MetastoreResource.class);
    classes.add(LoggingFilter.class);
    classes.add(MultiPartFeature.class);
    classes.add(LensApplicationListener.class);
    return classes;
  }
}
