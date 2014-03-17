package com.inmobi.grill.server;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.core.Application;

import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import com.inmobi.grill.server.metastore.MetastoreResource;
import com.inmobi.grill.server.query.QueryServiceResource;
import com.inmobi.grill.server.quota.QuotaResource;
import com.inmobi.grill.server.scheduler.ScheduleResource;
import com.inmobi.grill.server.session.SessionResource;

public class AllApps extends Application {

  public Set<Class<?>> getClasses() {
    final Set<Class<?>> classes = new HashSet<Class<?>>();
    // register root resource
    classes.add(SessionResource.class);
    classes.add(MetastoreResource.class);
    classes.add(QueryServiceResource.class);
    classes.add(QuotaResource.class);
    classes.add(ScheduleResource.class);
    classes.add(IndexResource.class);
    classes.add(MultiPartFeature.class);
    classes.add(LoggingFilter.class);
    classes.add(AuthenticationFilter.class);
    return classes;
}

}
