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
 * The Class QueryResultSetMetadata.
 */
@XmlRootElement
/**
 * Instantiates a new query result set metadata.
 *
 * @param columns
 *          the columns
 */
@AllArgsConstructor
/**
 * Instantiates a new query result set metadata.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class QueryResultSetMetadata {

  /** The columns. */
  @XmlElementWrapper
  @Getter
  private List<ResultColumn> columns;
}
