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
package org.apache.lens.cli.commands;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.lens.client.LensClient;
import org.apache.lens.client.LensClientSingletonWrapper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.impl.Indenter;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;
import org.codehaus.jackson.util.DefaultPrettyPrinter;
import org.springframework.shell.core.ExecutionProcessor;
import org.springframework.shell.event.ParseResult;

import com.google.common.collect.Sets;

/**
 * The Class BaseLensCommand.
 */
public class BaseLensCommand implements ExecutionProcessor {

  /** The mapper. */
  protected ObjectMapper mapper;

  /** The pp. */
  protected DefaultPrettyPrinter pp;

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(BaseLensCommand.class);

  /** The is connection active. */
  protected static boolean isConnectionActive;
  public static final String DATE_FMT = "yyyy-MM-dd'T'HH:mm:ss:SSS";

  public static final ThreadLocal<DateFormat> DATE_PARSER =
    new ThreadLocal<DateFormat>() {
      @Override
      protected SimpleDateFormat initialValue() {
        return new SimpleDateFormat(DATE_FMT);
      }
    };

  public static String formatDate(Date dt) {
    return DATE_PARSER.get().format(dt);
  }

  static {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        closeClientConnection();
      }
    });
  }

  /**
   * Close client connection.
   */
  protected static synchronized void closeClientConnection() {
    if (isConnectionActive) {
      LOG.debug("Request for stopping lens cli received");
      getClient().closeConnection();
      isConnectionActive = false;
    }
  }

  /**
   * Instantiates a new base lens command.
   */
  public BaseLensCommand() {
    getClient();
    mapper = new ObjectMapper();
    mapper.setSerializationInclusion(Inclusion.NON_NULL);
    mapper.setSerializationInclusion(Inclusion.NON_DEFAULT);
    pp = new DefaultPrettyPrinter();
    pp.indentObjectsWith(new Indenter() {
      @Override
      public void writeIndentation(JsonGenerator jg, int level) throws IOException, JsonGenerationException {
        if (level > 2) {
          jg.writeRaw("  ");
        } else {
          jg.writeRaw("\n");
        }
      }

      @Override
      public boolean isInline() {
        return false;
      }
    });
    isConnectionActive = true;
  }

  public void setClient(LensClient client) {
    getClientWrapper().setClient(client);
  }

  public static LensClient getClient() {
    return getClientWrapper().getClient();
  }

  public static LensClientSingletonWrapper getClientWrapper() {
    return LensClientSingletonWrapper.instance();
  }

  /**
   * Pretty printing JSON object into CLI String.
   *
   * @param json to be formatted
   * @return cli formatted string
   */
  public String formatJson(String json) {
    return json.replaceAll("\\[ \\{", "\n\n ").replaceAll("\\{", "").replaceAll("}", "").replaceAll("\\[", "")
      .replaceAll("]", "\n").replaceAll(",", "").replaceAll("\"", "").replaceAll("\n\n", "\n");
  }

  public String getValidPath(String path) {
    if (path.startsWith("~")) {
      path = path.replaceFirst("~", System.getProperty("user.home"));
    }
    File f = new File(path);
    if (!f.exists()) {
      throw new RuntimeException("Path " + path + " doesn't exist.");
    }
    return f.getAbsolutePath();
  }


  /**
   * This Code piece allows lens cli to be able to parse list arguments. It can already parse keyword args.
   * More details at https://github.com/spring-projects/spring-shell/issues/72
   * @param parseResult
   * @return
   */
  @Override
  public ParseResult beforeInvocation(ParseResult parseResult) {
    Object[] args = parseResult.getArguments();
    if (args != null && Sets.newHashSet(args).size() == 1) {
      if (args[0] instanceof String) {
        String[] split = ((String) args[0]).split("\\s+");
        Object[] newArgs = new String[args.length];
        System.arraycopy(split, 0, newArgs, 0, split.length);
        parseResult = new ParseResult(parseResult.getMethod(), parseResult.getInstance(), newArgs);
      }
    }
    return parseResult;
  }

  @Override
  public void afterReturningInvocation(ParseResult parseResult, Object o) {
  }

  @Override
  public void afterThrowingInvocation(ParseResult parseResult, Throwable throwable) {
  }
}
