package com.inmobi.grill.cli.commands;
/*
 * #%L
 * Grill CLI
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

import com.inmobi.grill.client.GrillClientSingletonWrapper;
import com.inmobi.grill.client.GrillClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.impl.Indenter;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.util.DefaultPrettyPrinter;
import java.io.IOException;

public class BaseGrillCommand {
  protected ObjectMapper mapper;
  protected DefaultPrettyPrinter pp;

  public static final Log LOG = LogFactory.getLog(BaseGrillCommand.class);
  protected static boolean isConnectionActive;

  {
    // force the singleton to be initialized
    getClient();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        closeClientConnection();
      }
    });
  }

  protected synchronized void closeClientConnection() {
    if (isConnectionActive) {
      LOG.debug("Request for stopping grill cli received");
      getClient().closeConnection();
      isConnectionActive = false;
    }
  }

  public BaseGrillCommand() {
    mapper = new ObjectMapper();
    pp = new DefaultPrettyPrinter();
    pp.indentObjectsWith(new Indenter() {
      @Override
      public void writeIndentation(JsonGenerator jg, int level)
          throws IOException,
          JsonGenerationException {
        if(level > 2) {
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

  public void setClient(GrillClient client) {
    getClientWrapper().setClient(client);
  }

  public GrillClient getClient() {
    return getClientWrapper().getClient();
  }

  public GrillClientSingletonWrapper getClientWrapper() {
    return GrillClientSingletonWrapper.INSTANCE;
  }
  /**
   * Pretty printing JSON object into CLI String
   * @param json to be formatted
   * @return cli formatted string
   */
  public String formatJson(String json) {
    return json.replaceAll("\\[ \\{","\n\n ").replaceAll("\\{","").
        replaceAll("}","").
        replaceAll("\\[", "").
        replaceAll("]","\n").replaceAll(",","").
        replaceAll("\"","").replaceAll("\n\n","\n");
  }
}
