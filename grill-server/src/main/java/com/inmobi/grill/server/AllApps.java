package com.inmobi.grill.server;

/*
 * #%L
 * Grill Server
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import com.inmobi.grill.server.metastore.MetastoreResource;
import com.inmobi.grill.server.query.QueryServiceResource;
import com.inmobi.grill.server.quota.QuotaResource;
import com.inmobi.grill.server.scheduler.ScheduleResource;
import com.inmobi.grill.server.session.SessionResource;

@ApplicationPath("/")
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
    classes.add(AuthenticationFilter.class);
    classes.add(GrillApplicationListener.class);
    classes.add(ConsistentStateFilter.class);
    classes.add(ServerModeFilter.class);
    return classes;
}

}
