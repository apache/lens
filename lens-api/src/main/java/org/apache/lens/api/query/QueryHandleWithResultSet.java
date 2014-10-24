/*
 * 
 */
package org.apache.lens.api.query;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * The Class QueryHandleWithResultSet.
 */
@XmlRootElement
/**
 * Instantiates a new query handle with result set.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class QueryHandleWithResultSet extends QuerySubmitResult {

  /** The query handle. */
  @XmlElement
  @Getter
  private QueryHandle queryHandle;

  /** The result. */
  @Getter
  @Setter
  private QueryResult result;

  /**
   * Instantiates a new query handle with result set.
   *
   * @param handle
   *          the handle
   */
  public QueryHandleWithResultSet(QueryHandle handle) {
    this.queryHandle = handle;
  }
}
