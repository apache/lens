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
package org.apache.lens.cube.error;

import static com.google.common.base.Preconditions.checkArgument;

import java.text.DateFormat;
import java.util.Date;
import java.util.TimeZone;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang3.StringUtils;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@EqualsAndHashCode
public class ColUnAvailableInTimeRange {

  private static final ThreadLocal<DateFormat> DATE_FORMAT = new ThreadLocal<DateFormat>() {
    @Override
    protected DateFormat initialValue() {
      DateFormat dateFormat = DateFormat.getDateTimeInstance(DateFormat.FULL, DateFormat.FULL);
      dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
      return dateFormat;
    }
  };

  @Getter
  @XmlElement
  private String columnName;
  @XmlElement
  private Long availableFromInMillisSinceEpoch;
  @XmlElement
  private Long availableTillInMillisSinceEpoch;

  public ColUnAvailableInTimeRange(final String columnName, final Long availableFromInMillisSinceEpoch,
      final Long availableTillInMillisSinceEpoch) {

    checkArgument(StringUtils.isNotBlank(columnName));

    /* One of start time or end time should be present.
     * A column with both start time and end time as null can never fall into category of ColUnAvailableInTimeRange */

    checkArgument(availableFromInMillisSinceEpoch != null || availableTillInMillisSinceEpoch != null);

    this.columnName = columnName;
    this.availableFromInMillisSinceEpoch = availableFromInMillisSinceEpoch;
    this.availableTillInMillisSinceEpoch = availableTillInMillisSinceEpoch;
  }

  public String getAvailability() {

    if (availableFromInMillisSinceEpoch != null && availableTillInMillisSinceEpoch != null) {

      return "after " + formatTime(availableFromInMillisSinceEpoch) + " and before "
          + formatTime(availableTillInMillisSinceEpoch);

    } else if (availableFromInMillisSinceEpoch != null) {

      return "after " + formatTime(availableFromInMillisSinceEpoch);
    } else {

      return "before " + formatTime(availableTillInMillisSinceEpoch);
    }
  }

  private String formatTime(final Long time) {
    return DATE_FORMAT.get().format(new Date(time));
  }
}
