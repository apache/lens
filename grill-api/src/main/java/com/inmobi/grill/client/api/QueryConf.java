package com.inmobi.grill.client.api;

import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement(name = "conf")
public class QueryConf {
  private Map<String, String> properties = new HashMap<String, String>();
  public QueryConf() {
    
  }

  public void addProperty(String key, String value) {
    properties.put(key, value);
  }

  public Map<String, String> getProperties() {
    return properties;
  }
}
