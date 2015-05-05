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
package org.apache.lens.regression.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class Util {

  private Util() {

  }

  private static final Logger LOGGER = Logger.getLogger(Util.class);
  private static final String PROPERTY_FILE = "lens.properties";
  private static Properties properties;

  public static synchronized Properties getPropertiesObj(String filename) {
    try {
      if (properties == null) {
        properties = new Properties();
        LOGGER.info("filename: " + filename);
        InputStream confStream = Util.class.getResourceAsStream("/" + filename);
        properties.load(confStream);
        confStream.close();
      }
      return properties;

    } catch (IOException e) {
      LOGGER.info(e.getStackTrace());
    }
    return null;
  }

  public static String getProperty(String property) {
    Properties prop = Util.getPropertiesObj(PROPERTY_FILE);
    return prop.getProperty(property);
  }

  public static String runRemoteCommand(String command) throws JSchException, IOException {
    StringBuilder outputBuffer = new StringBuilder();
    StringBuilder print = new StringBuilder();

    String userName = Util.getProperty("lens.remote.username");
    String host = Util.getProperty("lens.remote.host");
    String password = Util.getProperty("lens.remote.password");

    LOGGER.info("Running command : " + command + " on host : " + host + " with user as " + userName);

    JSch jsch = new JSch();
    Session session = jsch.getSession(userName, host, 22);
    session.setPassword(password);

    Properties config = new Properties();
    config.put("StrictHostKeyChecking", "no");

    session.setServerAliveInterval(10000);
    session.setConfig(config);
    session.connect(0);

    ChannelExec channel = (ChannelExec) session.openChannel("exec");
    InputStream commandOutput = channel.getInputStream();

    channel.setCommand(command);
    channel.connect();
    int readByte = commandOutput.read();
    char toAppend = ' ';
    while (readByte != 0xffffffff) {
      toAppend = (char) readByte;
      outputBuffer.append(toAppend);
      readByte = commandOutput.read();
      if (toAppend == '\n') {
        LOGGER.info(print.toString());
        print = new StringBuilder();
      } else {
        print.append(toAppend);
      }
    }
    channel.disconnect();
    session.disconnect();
    return outputBuffer.toString();
  }

  public static void writeFile(String fileName, String str) {
    try {
      PrintWriter out = new PrintWriter(fileName, "UTF-8");
      out.println(str);
      out.close();
    } catch (IOException e) {
      LOGGER.info("File Exception : " + e);
    }
  }

}
