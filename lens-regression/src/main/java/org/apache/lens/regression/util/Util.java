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

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import javax.ws.rs.core.Response;
import javax.xml.bind.*;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.StringList;
import org.apache.lens.api.metastore.ObjectFactory;

import org.apache.log4j.Logger;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;


public class Util {

  private static final Logger LOGGER = Logger.getLogger(Util.class);
  private static final String PROPERTY_FILE = "lens.properties";
  private static Properties properties;

  private Util() {

  }

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

  public static APIResult getApiResult(Response response) {
    APIResult result = response.readEntity(APIResult.class);
    return result;
  }

  @SuppressWarnings("unchecked")
  public static <T> Object extractObject(String queryString, Class<T> c) throws
      InstantiationException, IllegalAccessException {
    JAXBContext jaxbContext = null;
    Unmarshaller unmarshaller = null;
    StringReader reader = new StringReader(queryString);
    try {
      jaxbContext = JAXBContext.newInstance(ObjectFactory.class);
      unmarshaller = jaxbContext.createUnmarshaller();
      return (T) ((JAXBElement<?>) unmarshaller.unmarshal(reader)).getValue();
    } catch (JAXBException e) {
      System.out.println("Exception : " + e);
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> Object getObject(String queryString, Class<T> c) throws
      InstantiationException, IllegalAccessException {
    JAXBContext jaxbContext = null;
    Unmarshaller unmarshaller = null;
    StringReader reader = new StringReader(queryString);
    try {
      jaxbContext = JAXBContext.newInstance(c);
      unmarshaller = jaxbContext.createUnmarshaller();
      return (T) unmarshaller.unmarshal(reader);
    } catch (JAXBException e) {
      System.out.println("Exception : " + e);
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> String convertObjectToXml(T object, Class<T> clazz, String functionName) throws
      SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException,
      InvocationTargetException {
    JAXBElement<T> root = null;
    StringWriter stringWriter = new StringWriter();
    ObjectFactory methodObject = new ObjectFactory();

    Method method = methodObject.getClass().getMethod(functionName, clazz);
    root = (JAXBElement<T>) method.invoke(methodObject, object);
    try {
      getMarshaller(clazz).marshal(root, stringWriter);
    } catch (JAXBException e) {
      LOGGER.error("Not able to marshall", e);
    }
    return stringWriter.toString();
  }

  public static Marshaller getMarshaller(Class clazz) {
    JAXBContext jaxbContext = null;
    try {
      jaxbContext = JAXBContext.newInstance(clazz);
      Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
      jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

      return jaxbMarshaller;
    } catch (JAXBException e) {
      LOGGER.error("Error : ", e);
    }

    return null;
  }

  public static HashMap<String, String> stringListToMap(StringList stringList) {
    HashMap<String, String> map = new HashMap<String, String>();
    if (stringList == null) {
      return null;
    }
    List<String> list = stringList.getElements();
    for (String element : list) {
      StringTokenizer tk = new StringTokenizer(element, "=");
      map.put(tk.nextToken(), tk.nextToken());
    }
    return map;
  }

  public static HashMap<String, String> stringListToMap(String paramList) throws Exception {
    StringList stringList = (StringList) Util.getObject(paramList, StringList.class);
    return stringListToMap(stringList);
  }

}
