package com.inmobi.grill.server.scheduler;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

@ApplicationPath("/queryscheduler")
public class SchedulerApp extends Application {
  @Override
  public Set<Class<?>> getClasses() {
      final Set<Class<?>> classes = new HashSet<Class<?>>();
      // register root resource
      classes.add(ScheduleResource.class);
      return classes;
  }
}