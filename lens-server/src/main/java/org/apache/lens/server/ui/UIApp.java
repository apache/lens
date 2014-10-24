package org.apache.lens.server.ui;

import org.apache.lens.server.AuthenticationFilter;
import org.apache.lens.server.LensApplicationListener;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;

/**
 * The Class UIApp.
 */
@ApplicationPath("/ui")
public class UIApp extends Application {

  public Set<Class<?>> getClasses() {
    final Set<Class<?>> classes = new HashSet<Class<?>>();
    classes.add(StaticFileResource.class);
    classes.add(QueryServiceUIResource.class);
    classes.add(SessionUIResource.class);
    classes.add(MetastoreUIResource.class);
    classes.add(MultiPartFeature.class);
    classes.add(AuthenticationFilter.class);
    classes.add(LensApplicationListener.class);
    return classes;
  }

}
