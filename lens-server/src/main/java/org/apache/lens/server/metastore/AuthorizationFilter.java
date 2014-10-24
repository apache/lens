package org.apache.lens.server.metastore;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

public class AuthorizationFilter  implements Filter {

  @Override
  public void destroy() {
    // TODO Auto-generated method stub

  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain chain) throws IOException, ServletException {
    // TODO Auto-generated method stub
    chain.doFilter(request, response);
  }

  @Override
  public void init(FilterConfig config) throws ServletException {
    // TODO Auto-generated method stub

  }
}
