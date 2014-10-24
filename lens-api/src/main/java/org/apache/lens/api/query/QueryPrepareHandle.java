/*
 * 
 */
package org.apache.lens.api.query;

import java.util.UUID;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * The Class QueryPrepareHandle.
 */
@XmlRootElement
/**
 * Instantiates a new query prepare handle.
 *
 * @param prepareHandleId
 *          the prepare handle id
 */
@AllArgsConstructor
/**
 * Instantiates a new query prepare handle.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
/*
 * (non-Javadoc)
 *
 * @see java.lang.Object#hashCode()
 */
@EqualsAndHashCode(callSuper = false)
public class QueryPrepareHandle extends QuerySubmitResult {

  /** The prepare handle id. */
  @XmlElement
  @Getter
  private UUID prepareHandleId;

  /**
   * From string.
   *
   * @param handle
   *          the handle
   * @return the query prepare handle
   */
  public static QueryPrepareHandle fromString(String handle) {
    return new QueryPrepareHandle(UUID.fromString(handle));
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return prepareHandleId.toString();
  }
}
