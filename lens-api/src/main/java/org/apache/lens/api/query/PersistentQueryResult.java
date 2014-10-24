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
 * The Class PersistentQueryResult.
 */
@XmlRootElement
/**
 * Instantiates a new persistent query result.
 *
 * @param persistedURI
 *          the persisted uri
 * @param numRows
 *          the num rows
 */
@AllArgsConstructor
/**
 * Instantiates a new persistent query result.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class PersistentQueryResult extends QueryResult {

  /** The persisted uri. */
  @XmlElement
  @Getter
  private String persistedURI;

  /** The num rows. */
  @XmlElement
  @Getter
  private int numRows;
}
