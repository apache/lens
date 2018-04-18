/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lens.driver.hive;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;

import org.apache.lens.server.api.util.LensUtil;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.auth.KerberosSaslHelper;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.ICLIService;
import org.apache.hive.service.cli.thrift.RetryingThriftCLIServiceClient;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Class RetryingThriftCLIServiceClientSasl.
 *
 * This class can be used to connect to hive server using ThriftCLIServiceClient
 * with sasl transport protocol.
 *
 */
public class RetryingThriftCLIServiceClientSasl implements InvocationHandler {

  public static final Logger LOG = LoggerFactory.getLogger(RetryingThriftCLIServiceClientSasl.class);

  // base client to open thrift connection with client
  private ThriftCLIServiceClient base;

  // auto retry on connection failure.
  private final int retryLimit;

  // delay in retry post failure
  private final int retryDelaySeconds;

  // Hive client conf
  private HiveConf conf;

  // transport protocol to use
  private TTransport transport;

  public static class CLIServiceClientWrapperSasl extends RetryingThriftCLIServiceClient.CLIServiceClientWrapper {

    public CLIServiceClientWrapperSasl(ICLIService icliService, TTransport tTransport) {
      super(icliService, tTransport);
    }
  }

  private RetryingThriftCLIServiceClientSasl(HiveConf conf) {
    this.conf = conf;
    this.retryLimit = conf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_RETRY_LIMIT);
    this.retryDelaySeconds = (int) conf.getTimeVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_RETRY_DELAY_SECONDS,
    TimeUnit.SECONDS);
  }

  public static RetryingThriftCLIServiceClient.CLIServiceClientWrapper newRetryingCLIServiceClient(HiveConf conf)
          throws Exception {
    RetryingThriftCLIServiceClientSasl retryClient = new RetryingThriftCLIServiceClientSasl(conf);
    TTransport tTransport = retryClient
            .connectWithRetry(conf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_CONNECTION_RETRY_LIMIT));
    ICLIService cliService =
            (ICLIService) Proxy.newProxyInstance(RetryingThriftCLIServiceClientSasl.class.getClassLoader(),
                    CLIServiceClient.class.getInterfaces(), retryClient);
    return new RetryingThriftCLIServiceClient.CLIServiceClientWrapper(cliService, tTransport);
  }

  protected TTransport connectWithRetry(int retries) throws Exception {

    TTransportException exception = null;

    for (int i = 0; i < retries; i++) {
      try {
        return connect(conf);
      } catch (TTransportException e) {
        exception = e;
        LOG.warn("Connection attempt " + i, e);
      }
      try {
        Thread.sleep(retryDelaySeconds * 1000);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted", e);
      }
    }
    throw new HiveSQLException("Unable to connect after " + retries + " retries", exception);
  }

  protected synchronized TTransport connect(HiveConf conf) throws Exception {

    /*
      Can not get a renewed ugi in current thread spawned for hive client.
      Need to explicitly refresh token with keytab.
      This needs further investigation.
     */
    LensUtil.refreshLensTGT(conf);

    if (transport != null && transport.isOpen()) {
      transport.close();
    }

    String host = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST);
    int port = conf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT);
    LOG.info("Connecting to " + host + ":" + port);

    transport = new TSocket(host, port);
    ((TSocket) transport).setTimeout((int) conf.getTimeVar(HiveConf.ConfVars.SERVER_READ_SOCKET_TIMEOUT,
            TimeUnit.SECONDS) * 1000);

    try {
      ((TSocket) transport).getSocket().setKeepAlive(conf.getBoolVar(HiveConf.ConfVars.SERVER_TCP_KEEP_ALIVE));
    } catch (SocketException e) {
      LOG.error("Error setting keep alive to " + conf.getBoolVar(HiveConf.ConfVars.SERVER_TCP_KEEP_ALIVE), e);
    }

    try {
      Map<String, String> saslProps = new HashMap<String, String>();
      saslProps.put(Sasl.QOP, "auth-conf,auth-int,auth");
      saslProps.put(Sasl.SERVER_AUTH, "true");

      transport = KerberosSaslHelper.getKerberosTransport(
              conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL), host,
              transport, saslProps, false);
    } catch (SaslException e) {
      LOG.error("Error creating SASL transport", e);
    }

    TProtocol protocol = new TBinaryProtocol(transport);
    transport.open();

    LOG.info("Connected to " + host + ":" + port);

    base = new ThriftCLIServiceClient(new TCLIService.Client(protocol));
    return transport;
  }

  protected class InvocationResult {
    final boolean success;
    final Object result;
    final Throwable exception;

    InvocationResult(boolean success, Object result, Throwable exception) {
      this.success = success;
      this.result = result;
      this.exception = exception;
    }
  }

  protected InvocationResult invokeInternal(Method method, Object[] args) throws Throwable {
    InvocationResult result;
    try {
      Object methodResult = method.invoke(base, args);
      result = new InvocationResult(true, methodResult, null);
    } catch (UndeclaredThrowableException e) {
      throw e.getCause();
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof HiveSQLException) {
        HiveSQLException hiveExc = (HiveSQLException) e.getCause();
        Throwable cause = hiveExc.getCause();
        if ((cause instanceof TApplicationException)
                || (cause instanceof TProtocolException)
                || (cause instanceof TTransportException)) {
          result =new InvocationResult(false, null, hiveExc);
        } else {
          throw hiveExc;
        }
      } else {
        throw e.getCause();
      }
    }
    return result;
  }


  @Override
  public Object invoke(Object o, Method method, Object[] args) throws Throwable {
    int attempts = 0;

    while (true) {
      attempts++;
      InvocationResult invokeResult = invokeInternal(method, args);
      if (invokeResult.success) {
        return invokeResult.result;
      }

      // Error because of thrift client, we have to recreate base object
      connectWithRetry(conf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_CONNECTION_RETRY_LIMIT));

      if (attempts >=retryLimit) {
        LOG.error(method.getName() + " failed after " + attempts + " retries.", invokeResult.exception);
        throw invokeResult.exception;
      }

      LOG.warn("Last call ThriftCLIServiceClient." + method.getName() + " failed, attempts = " + attempts,
              invokeResult.exception);
      Thread.sleep(retryDelaySeconds * 1000);
    }
  }
}
