/*
 * 
 */
package org.apache.lens.api;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * The Class LensConf.
 */
@XmlRootElement(name = "conf")
/**
 * Instantiates a new lens conf.
 */
@NoArgsConstructor
public class LensConf implements Serializable {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = 1L;

  /** The properties. */
  @XmlElementWrapper
  @Getter
  private final Map<String, String> properties = new HashMap<String, String>();

  /**
   * Adds the property.
   *
   * @param key
   *          the key
   * @param value
   *          the value
   */
  public void addProperty(String key, String value) {
    properties.put(key, value);
  }
}
