/*
 * 
 */
package org.apache.lens.api.query;

import java.util.List;

import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * The Class ResultRow.
 */
@XmlRootElement
/**
 * Instantiates a new result row.
 *
 * @param values
 *          the values
 */
@AllArgsConstructor
/**
 * Instantiates a new result row.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ResultRow {

  /** The values. */
  @XmlElementWrapper
  @Getter
  List<Object> values;

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#toString()
   */
  public String toString() {
    return values.toString();
  }
}
