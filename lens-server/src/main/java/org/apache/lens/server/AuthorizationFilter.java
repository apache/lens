/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server;

import java.io.IOException;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.ext.Provider;
import javax.xml.ws.spi.http.HttpContext;

import org.apache.lens.server.api.authorization.ActionType;
import org.apache.lens.server.api.authorization.IAuthorizer;
import org.apache.lens.server.api.authorization.LensPrivilegeObject;
//import org.apache.lens.server.api.authorization.LensPrivilegeObject;

import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.filterchain.*;
import org.glassfish.grizzly.http.HttpContent;
import org.glassfish.grizzly.http.HttpRequestPacket;
import org.glassfish.grizzly.http.Method;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
@Provider
@PreMatching
public class AuthorizationFilter extends BaseFilter {

  private IAuthorizer authorizer;

  AuthorizationFilter(IAuthorizer authorizer) {
    this.authorizer = authorizer;
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public void onAdded(FilterChain filterChain) {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void onFilterChainChanged(FilterChain filterChain) {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void onRemoved(FilterChain filterChain) {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public NextAction handleRead(FilterChainContext ctx) throws IOException {
    System.out.println("HERE READ : ");
    System.out.println("ENTI DEVUDAA : "+ ctx.getMessage());
    System.out.println("CONTEXT filterchain: "+ ctx.getFilterChain());
    System.out.println("MESSAGE : "+ ((HttpContent)ctx.getMessage()).getHttpHeader() + " Body : "+ ((HttpContent)ctx.getMessage()).getContent());
    return ctx.getInvokeAction();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public NextAction handleWrite(FilterChainContext ctx) throws IOException {
    System.out.println("HERE WRITE : ");
    System.out.println("ENTI DEVUDAA : "+ ctx.getMessage());
    System.out.println("CONTEXT filterchain: "+ ctx.getFilterChain());
    System.out.println("MESSAGE : "+ ((HttpContent)ctx.getMessage()).getHttpHeader()  + " Body : "+ ((HttpContent)ctx.getMessage()).getContent());
    System.out.println("Query : " + ((HttpRequestPacket)((HttpContent)ctx.getMessage()).getHttpHeader()).getQueryString());
    System.out.println("Query : " +((HttpRequestPacket)((HttpContent)ctx.getMessage()).getHttpHeader()).getRequestURI());
    System.out.println("Query : " +((HttpRequestPacket)((HttpContent)ctx.getMessage()).getHttpHeader()).getMethod());
    final String user = "lens_user";

    final String queryString = ((HttpRequestPacket)((HttpContent)ctx.getMessage()).getHttpHeader()).getQueryString();
    final String[] split = queryString.split("/");

    final Method method = ((HttpRequestPacket)((HttpContent)ctx.getMessage()).getHttpHeader()).getMethod();
    final String resource = split[2];
    final String entity = split[3];


//    if(method.equals(Method.POST)) {
//      LensPrivilegeObject lensPrivilegeObject = new LensPrivilegeObject(LensPrivilegeObject.LensPrivilegeObjectType.valueOf(entity),  )
//      if(resource.equals("metastore")) {
//
//      } else if(resource.equals("queryapi")) {
//
//      }
//
//    } else if(method.equals(Method.PUT)) {
//      final String entityname = split[4];
//    }else {
//
//    }
    //authorizer.authorize();
    return ctx.getInvokeAction();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public NextAction handleConnect(FilterChainContext ctx) throws IOException {
    System.out.println("HERE CONNECT");
    return ctx.getInvokeAction();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public NextAction handleAccept(FilterChainContext ctx) throws IOException {
    System.out.println("HERE ACCEPT ");
//    System.out.println("ENTI DEVUDAA : "+ ctx.getMessage());
//    System.out.println("CONTEXT filterchain: "+ ctx.getFilterChain());
//    System.out.println("MESSAGE : "+ ctx.getMessage());
    return ctx.getInvokeAction();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public NextAction handleEvent(final FilterChainContext ctx,
    final FilterChainEvent event) throws IOException {
    System.out.println("HERE EVENT : "+ ctx.read()+" Event : "+ event.type() +" ewfw : "+ event.toString() );
    System.out.println("ENTI DEVUDAA : "+ ctx.getMessage());
    System.out.println("CONTEXT filterchain: "+ ctx.getFilterChain());
    System.out.println("MESSAGE : "+ ctx.getMessage());
    return ctx.getInvokeAction();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public NextAction handleClose(FilterChainContext ctx) throws IOException {
    System.out.println("HERE CLOSE");
    return ctx.getInvokeAction();
  }

  /**
   * Notification about exception, occurred on the {@link FilterChain}
   *
   * @param ctx event processing {@link FilterChainContext}
   * @param error error, which occurred during <tt>FilterChain</tt> execution
   */
  @Override
  public void exceptionOccurred(FilterChainContext ctx, Throwable error) {
  }


//  public FilterChainContext createContext(final Connection connection,
//    final FilterChainContext.Operation operation) {
//    FilterChain filterChain = (FilterChain) connection.getProcessor();
//    final FilterChainContext ctx =
//      filterChain.obtainFilterChainContext(connection);
//    final int idx = filterChain.indexOf(this);
//    ctx.setOperation(operation);
//    ctx.setFilterIdx(idx);
//    ctx.setStartIdx(idx);
//
//    return ctx;
//  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "@" + Integer.toHexString(hashCode());
  }
}
