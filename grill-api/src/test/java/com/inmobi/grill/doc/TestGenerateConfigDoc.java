package com.inmobi.grill.doc;

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


public class TestGenerateConfigDoc {
  public static final String SERVER_CONF_FILE = "../grill-server/src/main/resources/grill-default.xml";
  public static final String HIVE_DRIVER_CONF_FILE = "../grill-driver-hive/src/main/resources/hivedriver-default.xml";
  public static final String CLIENT_CONF_FILE = "../grill-client/src/main/resources/grill-client-default.xml";
  public static final String APT_FILE = "../src/site/apt/admin/config.apt";
  public static final String HIVE_DRIVER_APT_FILE = "../src/site/apt/admin/hivedriver-config.apt";
  public static final String CLIENT_APT_FILE = "../src/site/apt/user/client-config.apt";
  
  @Test
  public void generateServerConfigDoc() throws Exception {
    ConfigPrinter printer = new ConfigPrinter(SERVER_CONF_FILE, APT_FILE);
    printer.generateDoc("Grill server configuration");
  }

  @Test
  public void generateHivedriverConfigDoc() throws Exception {
    ConfigPrinter printer = new ConfigPrinter(HIVE_DRIVER_CONF_FILE, HIVE_DRIVER_APT_FILE);
    printer.generateDoc("Hive driver configuration");
  }

  @Test
  public void generateClientConfigDoc() throws Exception {
    ConfigPrinter printer = new ConfigPrinter(CLIENT_CONF_FILE, CLIENT_APT_FILE);
    printer.generateDoc("Grill client configuration");
  }

  class ConfigEntry {
    private String name;
    private String value;
    private String description;
    
    public void validate() throws IllegalArgumentException {
      if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("Name cannot be empty");
      }
      
      if (description == null || description.isEmpty()) {
        throw new IllegalArgumentException("Description cannot be empty for property: " + name);
      }
    }
    
    public String toString() {
      return name + ":" + value + ":" + description;
    }
  }
  
  class ConfigPrinter extends DefaultHandler {
    private final String configFile;
    private final String outputAPTFile;
    private StringBuilder buf;
    private ConfigEntry entry;
    private List<ConfigEntry> entries;
    boolean inProperty;
    
    public ConfigPrinter(String confFile, String outputAPTFile) {
      configFile = confFile;
      this.outputAPTFile = outputAPTFile;
      entries = new ArrayList<ConfigEntry>();
    }
    
    public void readConfigFile() throws IOException, ParserConfigurationException, SAXException {
      SAXParserFactory factory = SAXParserFactory.newInstance();
      SAXParser parser = factory.newSAXParser();
      parser.parse(new File(configFile), this);
    }
    
    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes)
        throws SAXException {
      if ("property".equalsIgnoreCase(qName)) {
        inProperty = true;
        entry = new ConfigEntry();
      }
      buf = new StringBuilder();
    }
    
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
    
    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
      buf.append(ch, start, length);
    }
    
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
    
    public void generateDoc(String heading) throws IOException, ParserConfigurationException, SAXException {
      readConfigFile();
      printAPT(heading);
    }
    
  }
  
}

