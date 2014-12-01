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
package org.apache.lens.api;

import lombok.*;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Date;

/**
 * The Class DateTime.
 */
@XmlRootElement
/**
 * Instantiates a new date.
 *
 * @param date
 *          the java utils date
 */
@AllArgsConstructor
/**
 * Instantiates a new date.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class DateTime {

  /**
   * The Date.
   */
  @Getter
  @Setter
  private Date date;
}