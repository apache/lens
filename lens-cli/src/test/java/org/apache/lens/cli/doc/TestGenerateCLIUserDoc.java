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
import java.util.HashSet;

import org.apache.lens.cli.commands.*;
import org.apache.lens.cli.commands.annotations.UserDocumentation;

import org.apache.commons.lang.StringUtils;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

public class TestGenerateCLIUserDoc {
  public static final String APT_FILE = "../src/site/apt/user/cli.apt";

  @Test
  public void generateDoc() throws IOException {
    BufferedWriter bw = new BufferedWriter(new FileWriter(new File(APT_FILE)));
    StringBuilder sb = new StringBuilder();
    sb.append(getCLIIntroduction()).append("\n\n\n");
    Class[] classes =
      new Class[]{LensConnectionCommands.class, LensDatabaseCommands.class, LensStorageCommands.class,
        LensCubeCommands.class, LensDimensionCommands.class, LensFactCommands.class, LensDimensionTableCommands.class,
        LensNativeTableCommands.class, LensQueryCommands.class, };
    for (Class claz : classes) {
      UserDocumentation doc = (UserDocumentation) claz.getAnnotation(UserDocumentation.class);
      if (doc != null && StringUtils.isNotBlank(doc.title())) {
        sb.append("** ").append(doc.title()).append("\n\n  ").append(doc.description()).append("\n\n");
      }
      sb.append("*--+--+\n"
        + "|<<Command>>|<<Description>>|\n"
        + "*--+--+\n");
      for (Method method : claz.getMethods()) {
        CliCommand annot = method.getAnnotation(CliCommand.class);
        if (annot == null) {
          continue;
        }
        sb.append("|");
        String sep = "";
        for (String value : annot.value()) {
          sb.append(sep).append(value);
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
                sb.append(" ");
                if (!cliOption.mandatory()) {
                  sb.append("[");
                }
                if (optional) {
                  sb.append("[");
                }
                sep = "";
                for (String key : keys) {
                  sb.append(sep).append("--").append(key);
                  sep = "/";
                }
                if (optional) {
                  sb.append("]");
                }
                sep = "";
              }
              sb.append(" ").append(cliOption.help().replaceAll("<", "\\\\<").replaceAll(">", "\\\\>"));
              if (!cliOption.mandatory()) {
                sb.append("]");
              }
            }
          }
        }
        sb.append("|").append(annot.help().replaceAll("<", "<<<").replaceAll(">", ">>>")).append("|").append("\n")
          .append("*--+--+\n");
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
    while((line = br.readLine()) != null) {
      sb.append(line).append("\n");
    }
    return sb;
  }

  private String getReadableName(String simpleName) {
    return simpleName.replaceAll("\\.class", "").replaceAll("[A-Z]", " $0").trim();
  }
}
