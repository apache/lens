/*
 * 
 */
package org.apache.lens.api.query;

import java.io.Serializable;
import java.util.UUID;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * The Class QueryHandle.
 */
@XmlRootElement
/**
 * Instantiates a new query handle.
 *
 * @param handleId
 *          the handle id
 */
@AllArgsConstructor
/**
 * Instantiates a new query handle.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
/*
 * (non-Javadoc)
 *
 * @see java.lang.Object#hashCode()
 */
@EqualsAndHashCode(callSuper = false)
public class QueryHandle extends QuerySubmitResult implements Serializable {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = 1L;

  /** The handle id. */
  @XmlElement
  @Getter
  private UUID handleId;

  /**
   * From string.
   *
   * @param handle
   *          the handle
   * @return the query handle
   */
  public static QueryHandle fromString(String handle) {
    return new QueryHandle(UUID.fromString(handle));
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return handleId.toString();
  }
}
