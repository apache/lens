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

import com.inmobi.grill.client.GrillClient;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.impl.Indenter;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.util.DefaultPrettyPrinter;

import java.io.IOException;

public class BaseGrillCommand {
  protected ObjectMapper mapper;
  protected DefaultPrettyPrinter pp;

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
  }
  protected GrillClient client;

  public void setClient(GrillClient client) {
    this.client = client;
  }

  public GrillClient getClient() {
    return this.client;
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
