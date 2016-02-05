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

import org.apache.lens.api.ToXMLString;
import org.apache.lens.api.util.PathValidator;
import org.apache.lens.client.LensClient;
import org.apache.lens.client.LensClientSingletonWrapper;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.impl.Indenter;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;
import org.codehaus.jackson.util.DefaultPrettyPrinter;
import org.jvnet.jaxb2_commons.lang.ToString;
import org.springframework.shell.core.ExecutionProcessor;
import org.springframework.shell.event.ParseResult;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;

/**
 * The Class BaseLensCommand.
 */
@Slf4j
public class BaseLensCommand implements ExecutionProcessor {
  public static final String LENS_CLI_PREFIX = "lens.cli.";
  public static final String JSON_PRETTY_SUFFIX = "json.pretty";

  /** The mapper. */
  protected ObjectMapper mapper;

  /** The pp. */
  protected DefaultPrettyPrinter pp;

  /** The is connection active. */
  protected static boolean isConnectionActive;
  public static final String DATE_FMT = "yyyy-MM-dd'T'HH:mm:ss:SSS";

  private LensClient lensClient = null;

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
      log.debug("Request for stopping lens cli received");
      getClientWrapper().getClient().closeConnection();
      isConnectionActive = false;
    }
  }

  /**
   * Instantiates a new base lens command.
   */
  public BaseLensCommand() {
    mapper = new ObjectMapper();
    mapper.setSerializationInclusion(Inclusion.NON_NULL);
    mapper.setSerializationInclusion(Inclusion.NON_DEFAULT);
    pp = new DefaultPrettyPrinter();
    pp.indentObjectsWith(new Indenter() {
      @Override
      public void writeIndentation(JsonGenerator jg, int level) throws IOException {
        jg.writeRaw("\n");
        for (int i = 0; i < level; i++) {
          jg.writeRaw(" ");
        }
      }

      @Override
      public boolean isInline() {
        return false;
      }
    });
  }

  public void setClient(LensClient client) {
    lensClient = client;
  }

  public LensClient getClient() {
    if (lensClient == null) {
      setClient(getClientWrapper().getClient());
      isConnectionActive = true;
    }
    return lensClient;
  }

  public static LensClientSingletonWrapper getClientWrapper() {
    return LensClientSingletonWrapper.instance();
  }

  /**
   * Pretty printing JSON object into CLI String.
   *
   * @param data to be formatted
   * @return cli formatted string
   */
  public String formatJson(Object data) {
    try {
      if (data instanceof ToString || data instanceof ToXMLString) {
        return data.toString();
      }
      String json = mapper.writer(pp).writeValueAsString(data);
      JsonNode tree = mapper.valueToTree(data);
      System.out.println(tree);
      if (getClient().getConf().getBoolean(LENS_CLI_PREFIX + JSON_PRETTY_SUFFIX, false)) {
        return json;
      }
      return json.replaceAll("\\[ \\{", "\n\n ").replaceAll("\\{", "").replaceAll("}", "").replaceAll("\\[", "")
        .replaceAll("]", "\n").replaceAll(",", "").replaceAll("\"", "").replaceAll("\n\n", "\n");
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
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

  /**
   * Method that uses PathValidator to get appropriate path.
   *
   * @param path
   * @param shouldBeDirectory
   * @param shouldExist
   * @return
   */
  public String getValidPath(File path, boolean shouldBeDirectory, boolean shouldExist) {
    PathValidator pathValidator = getClient().getPathValidator();
    return pathValidator.getValidPath(path, shouldBeDirectory, shouldExist);
  }

  /**
   * Method to remove unrequired prefix from path.
   *
   * @param path
   * @return
   */
  public String removePrefixBeforeURI(String path) {
    PathValidator pathValidator = getClient().getPathValidator();
    return pathValidator.removePrefixBeforeURI(path);
  }

  public String getOrDefaultQueryHandleString(String queryHandleString) {
    if (queryHandleString != null) {
      return queryHandleString;
    }
    if (getClient().getStatement().getQuery() != null) {
      return getClient().getStatement().getQueryHandleString();
    }
    throw new IllegalArgumentException("Query handle not provided and no queries interacted with in the session.");
  }
}
