package com.inmobi.grill.server;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hive.service.Service;

public class ConsistentStateFilter  implements Filter {

  public static final Log LOG = LogFactory.getLog(AuthenticationFilter.class);

  @Override
  public void destroy() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain chain) throws IOException, ServletException {
    if (GrillServices.get().getServiceState().equals(Service.STATE.STARTED) &&
        !GrillServices.get().isStopping() ) {
      chain.doFilter(request, response);
    } else {
      throw new IllegalStateException("Server is going down");
    }
  }

  @Override
  public void init(FilterConfig config) throws ServletException {
    // TODO Auto-generated method stub
    
  }


}
