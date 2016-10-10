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
package org.apache.lens.cli.doc;

import java.io.*;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.*;

import org.apache.lens.cli.commands.*;
import org.apache.lens.cli.commands.annotations.UserDocumentation;

import org.apache.commons.lang.StringUtils;

import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestGenerateCLIUserDoc {
  public static final String APT_FILE = "../src/site/apt/user/cli.apt";
  @Data
  static class DocEntry {
    private String command;
    private String[] help;
    private int maxLength = 0;
    public DocEntry(String command, String help) {
      this.command = command;
      this.help = help.split("\r|\n");
      for (int i = 0; i < this.help.length; i++) {
        this.help[i] = this.help[i].replaceAll("\\|", "\\\\|");
        if (i > 0) {
          this.help[i] = this.help[i].replaceAll("\\ ", "\\\\ ");
        }
      }
      for (String line: this.help) {
        if (line.length() > maxLength) {
          maxLength = line.length();
        }
      }
      for (int i = 0; i < this.help.length; i++) {
        StringBuilder sb = new StringBuilder(this.help[i]);
        while (sb.length() < maxLength) {
          sb.append(" ");
        }
        this.help[i] = sb.append("\\ ").toString();
      }
    }
  }
  @Test
  public void generateDoc() throws IOException {
    BufferedWriter bw = new BufferedWriter(new FileWriter(new File(APT_FILE)));
    StringBuilder sb = new StringBuilder();
    sb.append(getCLIIntroduction()).append("\n\n\n");
    List<Class<? extends CommandMarker>> classes = Lists.newArrayList(
      LensConnectionCommands.class,
      LensDatabaseCommands.class,
      LensStorageCommands.class,
      LensCubeCommands.class,
      LensDimensionCommands.class,
      LensFactCommands.class,
      LensDimensionTableCommands.class,
      LensNativeTableCommands.class,
      LensQueryCommands.class,
      LensLogResourceCommands.class,
      LensSchemaCommands.class
    );

    for (Class claz : classes) {
      UserDocumentation doc = (UserDocumentation) claz.getAnnotation(UserDocumentation.class);
      if (doc != null && StringUtils.isNotBlank(doc.title())) {
        sb.append("** ").append(doc.title()).append("\n\n  ").append(doc.description()).append("\n\n");
      }
      sb.append("*--+--+\n"
        + "|<<Command>>|<<Description>>|\n"
        + "*--+--+\n");
      TreeSet<Method> methods = Sets.newTreeSet(new Comparator<Method>() {
        @Override
        public int compare(Method o1, Method o2) {
          return o1.getAnnotation(CliCommand.class).value()[0].compareTo(o2.getAnnotation(CliCommand.class).value()[0]);
        }
      });

      for (Method method : claz.getMethods()) {
        if (method.getAnnotation(CliCommand.class) != null) {
          methods.add(method);
        } else {
          log.info("Not adding " + method.getDeclaringClass().getSimpleName() + "#" + method.getName());
        }
      }
      List<DocEntry> docEntries = Lists.newArrayList();
      for (Method method : methods) {
        CliCommand annot = method.getAnnotation(CliCommand.class);
        StringBuilder commandBuilder = new StringBuilder();
        String sep = "";
        for (String value : annot.value()) {
          commandBuilder.append(sep).append(value);
          sep = "/";
        }
        for (Annotation[] annotations : method.getParameterAnnotations()) {
          for (Annotation paramAnnot : annotations) {
            if (paramAnnot instanceof CliOption) {
              CliOption cliOption = (CliOption) paramAnnot;
              HashSet<String> keys = Sets.newHashSet(cliOption.key());
              boolean optional = false;
              if (keys.contains("")) {
                optional = true;
                keys.remove("");
              }
              if (!keys.isEmpty()) {
                commandBuilder.append(" ");
                if (!cliOption.mandatory()) {
                  commandBuilder.append("[");
                }
                if (optional) {
                  commandBuilder.append("[");
                }
                sep = "";
                for (String key : keys) {
                  commandBuilder.append(sep).append("--").append(key);
                  sep = "/";
                }
                if (optional) {
                  commandBuilder.append("]");
                }
                sep = "";
              }
              commandBuilder.append(" ").append(cliOption.help().replaceAll("<", "\\\\<").replaceAll(">", "\\\\>"));
              if (!cliOption.mandatory()) {
                commandBuilder.append("]");
              }
            }
          }
        }
        docEntries.add(new DocEntry(commandBuilder.toString(),
          annot.help().replaceAll("<", "<<<").replaceAll(">", ">>>")));
      }
      for (DocEntry entry: docEntries) {
        for (int i = 0; i < entry.getHelp().length; i++) {
          sb.append("|").append(i == 0 ? entry.getCommand() : entry.getCommand().replaceAll(".", " "))
            .append("|").append(entry.getHelp()[i]).append("|").append("\n");
        }
        sb.append("*--+--+\n");
      }
      sb.append("  <<").append(getReadableName(claz.getSimpleName())).append(">>\n\n===\n\n");
    }
    bw.write(sb.toString());
    bw.close();
  }

  private StringBuilder getCLIIntroduction() throws IOException {
    BufferedReader br =
      new BufferedReader(new InputStreamReader(TestGenerateCLIUserDoc.class.getResourceAsStream("/cli-intro.apt")));
    StringBuilder sb = new StringBuilder();
    String line;
    while ((line = br.readLine()) != null) {
      sb.append(line).append("\n");
    }
    return sb;
  }

  private String getReadableName(String simpleName) {
    return simpleName.replaceAll("\\.class", "").replaceAll("[A-Z]", " $0").trim();
  }
}
