package com.inmobi.grill.common;

import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.Getter;
import lombok.NoArgsConstructor;

@XmlRootElement(name = "conf")
@NoArgsConstructor
public class GrillConf {
  @XmlElementWrapper @Getter
  private final Map<String, String> properties = new HashMap<String, String>();

  public void addProperty(String key, String value) {
    properties.put(key, value);
  }
}
