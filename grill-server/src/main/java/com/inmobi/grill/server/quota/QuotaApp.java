package com.inmobi.grill.server.quota;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import org.glassfish.jersey.filter.LoggingFilter;

import com.inmobi.grill.server.GrillApplicationListener;

@ApplicationPath("/quota")
public class QuotaApp  extends Application {
  @Override
  public Set<Class<?>> getClasses() {
      final Set<Class<?>> classes = new HashSet<Class<?>>();
      // register root resource
      classes.add(QuotaResource.class);
      classes.add(GrillApplicationListener.class);
      classes.add(LoggingFilter.class);
      return classes;
  }
}