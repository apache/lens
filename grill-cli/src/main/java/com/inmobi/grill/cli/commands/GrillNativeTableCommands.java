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

import com.google.common.base.Joiner;
import com.inmobi.grill.api.metastore.*;
import com.inmobi.grill.client.GrillClient;

import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.StringWriter;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

@Component
public class GrillNativeTableCommands implements CommandMarker {
  private GrillClient client;
  private ObjectFactory objectFactory = new ObjectFactory();


  public void setClient(GrillClient client) {
    this.client = client;
  }

  @CliCommand(value = "show nativetables", help = "show list of native tables")
  public String showNativeTables() {
    List<String> nativetables = client.getAllNativeTables();
    if( nativetables != null) {
      return Joiner.on("\n").join(nativetables);
    } else {
      return "No native tables found";
    }
  }

  @CliCommand(value = "describe nativetable", help = "describe nativetable")
  public String describeNativeTable(@CliOption(key = {"", "nativetable"},
  mandatory = true, help = "<native-table-name>") String tblName) {

    NativeTable nativetable = client.getNativeTable(tblName);
    try {
      JAXBContext ctx = JAXBContext.newInstance(NativeTable.class);
      StringWriter stringWriter = new StringWriter();
      Marshaller marshaller = ctx.createMarshaller();
      marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
      marshaller.marshal(objectFactory.createNativeTable(nativetable), stringWriter);
      return stringWriter.toString();
    } catch (JAXBException e) {
      throw new IllegalArgumentException(e);
    } finally {

    }
  }
}
