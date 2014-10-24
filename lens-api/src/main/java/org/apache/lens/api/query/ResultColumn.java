/*
 * 
 */
package org.apache.lens.api.query;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * The Class ResultColumn.
 */
@XmlRootElement
/**
 * Instantiates a new result column.
 *
 * @param name
 *          the name
 * @param type
 *          the type
 */
@AllArgsConstructor
/**
 * Instantiates a new result column.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ResultColumn {

  /** The name. */
  @XmlElement
  @Getter
  private String name;

  /** The type. */
  @XmlElement
  @Getter
  private ResultColumnType type;

  /**
   * Instantiates a new result column.
   *
   * @param name
   *          the name
   * @param type
   *          the type
   */
  public ResultColumn(String name, String type) {
    this(name, ResultColumnType.valueOf(type.toUpperCase()));
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return new StringBuilder(name).append(':').append(type).toString();
  }
}
