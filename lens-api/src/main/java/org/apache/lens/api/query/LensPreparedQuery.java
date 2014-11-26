/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * 
 */
package org.apache.lens.api.query;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.lens.api.LensConf;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Date;

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

  /**
   * The prepare handle.
   */
  @XmlElement
  @Getter
  private QueryPrepareHandle prepareHandle;

  /**
   * The user query.
   */
  @XmlElement
  @Getter
  private String userQuery;

  /**
   * The prepared time.
   */
  @XmlElement
  @Getter
  private Date preparedTime;

  /**
   * The prepared user.
   */
  @XmlElement
  @Getter
  private String preparedUser;

  /**
   * The selected driver class name.
   */
  @XmlElement
  @Getter
  private String selectedDriverClassName;

  /**
   * The driver query.
   */
  @XmlElement
  @Getter
  private String driverQuery;

  /**
   * The conf.
   */
  @XmlElement
  @Getter
  private LensConf conf;
}
