package com.inmobi.grill.service;

import java.io.IOException;
import java.util.UUID;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.NDC;

import com.inmobi.grill.query.service.QueryExecutionServiceImpl;

public class AuthenticationFilter implements Filter {

  public static final Log LOG = LogFactory.getLog(AuthenticationFilter.class);

  @Override
  public void destroy() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain chain) throws IOException, ServletException {
    // TODO Auto-generated method stub
    if (!(request instanceof HttpServletRequest) || !(response instanceof HttpServletResponse)) {
      throw new IllegalStateException("Invalid request/response object");
  }
  HttpServletRequest httpRequest = (HttpServletRequest) request;
  HttpServletResponse httpResponse = (HttpServletResponse) response;

  String requestId = UUID.randomUUID().toString();
  String user = httpRequest.getHeader("Remote-User");

  try {
    NDC.push(user + ":" + httpRequest.getMethod() + "/" + httpRequest.getPathInfo());
    NDC.push(requestId);
    LOG.info("Request from user: " + user + ", path=" + httpRequest.getPathInfo()
            + ", query=" + httpRequest.getQueryString());
    chain.doFilter(request, response);
} finally {
    NDC.pop();
    NDC.pop();
}
  }

  @Override
  public void init(FilterConfig config) throws ServletException {
    // TODO Auto-generated method stub
    
  }

}
