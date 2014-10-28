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
/*
 * 
 */
package org.apache.lens.doc;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.testng.annotations.Test;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * The Class TestGenerateConfigDoc.
 */
public class TestGenerateConfigDoc {

  /** The Constant SERVER_CONF_FILE. */
  public static final String SERVER_CONF_FILE = "../lens-server/src/main/resources/lensserver-default.xml";

  /** The Constant SESSION_CONF_FILE. */
  public static final String SESSION_CONF_FILE = "../lens-server/src/main/resources/lenssession-default.xml";

  /** The Constant HIVE_DRIVER_CONF_FILE. */
  public static final String HIVE_DRIVER_CONF_FILE = "../lens-driver-hive/src/main/resources/hivedriver-default.xml";

  /** The Constant JDBC_DRIVER_CONF_FILE. */
  public static final String JDBC_DRIVER_CONF_FILE = "../lens-driver-jdbc/src/main/resources/jdbcdriver-default.xml";

  /** The Constant CLIENT_CONF_FILE. */
  public static final String CLIENT_CONF_FILE = "../lens-client/src/main/resources/lens-client-default.xml";

  /** The Constant CUBE_QUERY_CONF_FILE. */
  public static final String CUBE_QUERY_CONF_FILE = "../lens-cube/src/main/resources/olap-query-conf.xml";

  /** The Constant APT_FILE. */
  public static final String APT_FILE = "../src/site/apt/admin/config.apt";

  /** The Constant SESSION_APT_FILE. */
  public static final String SESSION_APT_FILE = "../src/site/apt/admin/session-config.apt";

  /** The Constant HIVE_DRIVER_APT_FILE. */
  public static final String HIVE_DRIVER_APT_FILE = "../src/site/apt/admin/hivedriver-config.apt";

  /** The Constant JDBC_DRIVER_APT_FILE. */
  public static final String JDBC_DRIVER_APT_FILE = "../src/site/apt/admin/jdbcdriver-config.apt";

  /** The Constant CLIENT_APT_FILE. */
  public static final String CLIENT_APT_FILE = "../src/site/apt/user/client-config.apt";

  /** The Constant CUBE_QUERY_CONF_APT_FILE. */
  public static final String CUBE_QUERY_CONF_APT_FILE = "../src/site/apt/user/olap-query-conf.apt";

  /**
   * Generate server config doc.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void generateServerConfigDoc() throws Exception {
    ConfigPrinter printer = new ConfigPrinter(SERVER_CONF_FILE, APT_FILE);
    printer.generateDoc("Lens server configuration");
  }

  /**
   * Generate session config doc.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void generateSessionConfigDoc() throws Exception {
    ConfigPrinter printer = new ConfigPrinter(SESSION_CONF_FILE, SESSION_APT_FILE);
    printer.generateDoc("Lens session configuration");
  }

  /**
   * Generate hivedriver config doc.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void generateHivedriverConfigDoc() throws Exception {
    ConfigPrinter printer = new ConfigPrinter(HIVE_DRIVER_CONF_FILE, HIVE_DRIVER_APT_FILE);
    printer.generateDoc("Hive driver configuration");
  }

  /**
   * Generate jdbcdriver config doc.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void generateJdbcdriverConfigDoc() throws Exception {
    ConfigPrinter printer = new ConfigPrinter(JDBC_DRIVER_CONF_FILE, JDBC_DRIVER_APT_FILE);
    printer.generateDoc("Jdbc driver configuration");
  }

  /**
   * Generate client config doc.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void generateClientConfigDoc() throws Exception {
    ConfigPrinter printer = new ConfigPrinter(CLIENT_CONF_FILE, CLIENT_APT_FILE);
    printer.generateDoc("Lens client configuration");
  }

  /**
   * Generate olap query config doc.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void generateOLAPQueryConfigDoc() throws Exception {
    ConfigPrinter printer = new ConfigPrinter(CUBE_QUERY_CONF_FILE, CUBE_QUERY_CONF_APT_FILE);
    printer.generateDoc("OLAP query configuration");
  }

  /**
   * The Class ConfigEntry.
   */
  class ConfigEntry {

    /** The name. */
    private String name;

    /** The value. */
    private String value;

    /** The description. */
    private String description;

    /**
     * Validate.
     *
     * @throws IllegalArgumentException
     *           the illegal argument exception
     */
    public void validate() throws IllegalArgumentException {
      if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("Name cannot be empty");
      }

      if (description == null || description.isEmpty()) {
        throw new IllegalArgumentException("Description cannot be empty for property: " + name);
      }
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    public String toString() {
      return name + ":" + value + ":" + description;
    }
  }

  /**
   * The Class ConfigPrinter.
   */
  class ConfigPrinter extends DefaultHandler {

    /** The config file. */
    private final String configFile;

    /** The output apt file. */
    private final String outputAPTFile;

    /** The buf. */
    private StringBuilder buf;

    /** The entry. */
    private ConfigEntry entry;

    /** The entries. */
    private List<ConfigEntry> entries;

    /** The in property. */
    boolean inProperty;

    /**
     * Instantiates a new config printer.
     *
     * @param confFile
     *          the conf file
     * @param outputAPTFile
     *          the output apt file
     */
    public ConfigPrinter(String confFile, String outputAPTFile) {
      configFile = confFile;
      this.outputAPTFile = outputAPTFile;
      entries = new ArrayList<ConfigEntry>();
    }

    /**
     * Read config file.
     *
     * @throws IOException
     *           Signals that an I/O exception has occurred.
     * @throws ParserConfigurationException
     *           the parser configuration exception
     * @throws SAXException
     *           the SAX exception
     */
    public void readConfigFile() throws IOException, ParserConfigurationException, SAXException {
      SAXParserFactory factory = SAXParserFactory.newInstance();
      SAXParser parser = factory.newSAXParser();
      parser.parse(new File(configFile), this);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.xml.sax.helpers.DefaultHandler#startElement(java.lang.String, java.lang.String, java.lang.String,
     * org.xml.sax.Attributes)
     */
    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
      if ("property".equalsIgnoreCase(qName)) {
        inProperty = true;
        entry = new ConfigEntry();
      }
      buf = new StringBuilder();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.xml.sax.helpers.DefaultHandler#endElement(java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
      if (inProperty && "name".equalsIgnoreCase(qName)) {
        entry.name = buf.toString();
      } else if (inProperty && "value".equalsIgnoreCase(qName)) {
        entry.value = buf.toString();
        if (entry.value == null || entry.value.isEmpty()) {
          entry.value = " ";
        }
      } else if (inProperty && "description".equalsIgnoreCase(qName)) {
        // replace new lines with space
        entry.description = buf.toString().replaceAll("\\r|\\n", "");
      } else if (inProperty && "property".equalsIgnoreCase(qName)) {
        entry.validate();
        entries.add(entry);
        inProperty = false;
      }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.xml.sax.helpers.DefaultHandler#characters(char[], int, int)
     */
    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
      buf.append(ch, start, length);
    }

    /**
     * Prints the apt.
     *
     * @param heading
     *          the heading
     * @throws IOException
     *           Signals that an I/O exception has occurred.
     */
    public void printAPT(String heading) throws IOException {
      PrintWriter apt = null;
      try {
        apt = new PrintWriter(new FileWriter(outputAPTFile));
        Collections.sort(entries, new Comparator<ConfigEntry>() {

          @Override
          public int compare(ConfigEntry e1, ConfigEntry e2) {
            return e1.name.compareTo(e2.name);
          }

        });
        // Add license
        apt.println("~~");
        apt.println("~~ Licensed to the Apache Software Foundation (ASF) under one");
        apt.println("~~ or more contributor license agreements.  See the NOTICE file");
        apt.println("~~ distributed with this work for additional information");
        apt.println("~~ regarding copyright ownership.  The ASF licenses this file");
        apt.println("~~ to you under the Apache License, Version 2.0 (the");
        apt.println("~~ \"License\"); you may not use this file except in compliance");
        apt.println("~~ with the License.  You may obtain a copy of the License at");
        apt.println("~~");
        apt.println("~~   http://www.apache.org/licenses/LICENSE-2.0");
        apt.println("~~");
        apt.println("~~ Unless required by applicable law or agreed to in writing,");
        apt.println("~~ software distributed under the License is distributed on an");
        apt.println("~~ \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY");
        apt.println("~~ KIND, either express or implied.  See the License for the");
        apt.println("~~ specific language governing permissions and limitations");
        apt.println("~~ under the License.");
        apt.println("~~");
        apt.println("");

        // Print header
        apt.println(heading);
        apt.println();

        apt.println("===");

        apt.println();

        // Print config entries
        int i = 1;
        apt.println("*--+--+---+--+");
        apt.println("|<<No.>>|<<Property Name>>|<<Default Value>>|<<Description>>|");
        for (ConfigEntry entry : entries) {
          apt.println("*--+--+---+--+");
          apt.print("|");
          apt.print(i++);
          apt.print("|");
          apt.print(entry.name);
          apt.print("|");
          apt.print(entry.value);
          apt.print("|");
          apt.print(entry.description == null ? "" : entry.description);
          apt.println("|");
        }
        apt.println("*--+--+---+--+");
        apt.println("The configuration parameters and their default values");
        apt.flush();
      } finally {
        if (apt != null) {
          apt.close();
        }
      }
    }

    /**
     * Generate doc.
     *
     * @param heading
     *          the heading
     * @throws IOException
     *           Signals that an I/O exception has occurred.
     * @throws ParserConfigurationException
     *           the parser configuration exception
     * @throws SAXException
     *           the SAX exception
     */
    public void generateDoc(String heading) throws IOException, ParserConfigurationException, SAXException {
      readConfigFile();
      printAPT(heading);
    }

  }

}
