/*
 * 
 */
package org.apache.lens.api.query;

import java.util.Date;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.lens.api.LensConf;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * The Class LensPreparedQuery.
 */
@XmlRootElement
/**
 * Instantiates a new lens prepared query.
 *
 * @param prepareHandle
 *          the prepare handle
 * @param userQuery
 *          the user query
 * @param preparedTime
 *          the prepared time
 * @param preparedUser
 *          the prepared user
 * @param selectedDriverClassName
 *          the selected driver class name
 * @param driverQuery
 *          the driver query
 * @param conf
 *          the conf
 */
@AllArgsConstructor
/**
 * Instantiates a new lens prepared query.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class LensPreparedQuery {

  /** The prepare handle. */
  @XmlElement
  @Getter
  private QueryPrepareHandle prepareHandle;

  /** The user query. */
  @XmlElement
  @Getter
  private String userQuery;

  /** The prepared time. */
  @XmlElement
  @Getter
  private Date preparedTime;

  /** The prepared user. */
  @XmlElement
  @Getter
  private String preparedUser;

  /** The selected driver class name. */
  @XmlElement
  @Getter
  private String selectedDriverClassName;

  /** The driver query. */
  @XmlElement
  @Getter
  private String driverQuery;

  /** The conf. */
  @XmlElement
  @Getter
  private LensConf conf;
}
