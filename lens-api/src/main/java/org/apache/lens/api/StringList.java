/*
 * 
 */
package org.apache.lens.api;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * The Class StringList.
 */
@XmlRootElement
/**
 * Instantiates a new string list.
 *
 * @param elements
 *          the elements
 */
@AllArgsConstructor
/**
 * Instantiates a new string list.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class StringList {

  /** The elements. */
  @Getter
  @Setter
  private List<String> elements;
}
