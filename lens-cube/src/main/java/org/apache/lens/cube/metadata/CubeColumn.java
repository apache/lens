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
package org.apache.lens.cube.metadata;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import com.google.common.base.Optional;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class CubeColumn implements Named {

  private final String name;
  private final Date startTime;
  private final Date endTime;
  private final Double cost;
  private final String description;
  private final String displayString;
  @Getter
  private final Map<String, String> tags;

  static final ThreadLocal<DateFormat> COLUMN_TIME_FORMAT =
    new ThreadLocal<DateFormat>() {
      @Override
      protected SimpleDateFormat initialValue() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf;
      }
    };

  public CubeColumn(String name , String description, String displayString,
                    Date startTime, Date endTime, Double cost) {
    this(name, description, displayString, startTime, endTime, cost, new HashMap<String, String>());
  }

  public CubeColumn(String name, String description, String displayString,
                    Date startTime, Date endTime, Double cost, Map<String, String> tags) {
    assert (name != null);
    this.name = name.toLowerCase();
    this.startTime = startTime;
    this.endTime = endTime;
    this.cost = cost;
    this.description = description;
    this.displayString = displayString;
    this.tags = tags;
  }

  private Date getDate(String propKey, Map<String, String> props) {
    String timeStr = props.get(propKey);
    return getDate(timeStr);
  }

  protected Date getDate(String timeStr) {
    if (timeStr != null) {
      try {
        return COLUMN_TIME_FORMAT.get().parse(timeStr);
      } catch (Exception e) {
        // ignore and return null
        log.warn("Column time passed:{} is not parsable, its ignored", timeStr, e);
      }
    }
    return null;
  }

  private Double getDouble(String propKey, Map<String, String> props) {
    String doubleStr = props.get(propKey);
    if (doubleStr != null) {
      try {
        return Double.parseDouble(doubleStr);
      } catch (Exception e) {
        // ignore and return null
        log.warn("Property {} value {} is not parsable, its ignored", propKey, doubleStr, e);
      }
    }
    return null;
  }

  private static synchronized void addTagProperties(String name, Map<String, String> props, Map<String, String> tags) {
    String colName = MetastoreUtil.getCubeColTagKey(name);
    if (tags != null) {
      for (Map.Entry<String, String> entry : tags.entrySet()) {
        props.put(colName.concat(entry.getKey()), entry.getValue());
      }
    }
  }

  private static Map<String, String> getColumnTags(String propKey, Map<String, String> props) {
    Map<String, String> tagProp = new HashMap<>();
    for (String key : props.keySet()) {
      if (key.startsWith(propKey)) {
        tagProp.put(key.replace(propKey, ""), props.get(key).replace(propKey, ""));
      }
    }
    return tagProp;
  }

  public CubeColumn(String name, Map<String, String> props) {
    this.name = name;
    this.startTime = getDate(MetastoreUtil.getCubeColStartTimePropertyKey(name), props);
    this.endTime = getDate(MetastoreUtil.getCubeColEndTimePropertyKey(name), props);
    this.cost = getDouble(MetastoreUtil.getCubeColCostPropertyKey(name), props);
    this.description = props.get(MetastoreUtil.getCubeColDescriptionKey(name));
    this.displayString = props.get(MetastoreUtil.getCubeColDisplayKey(name));
    this.tags = getColumnTags(MetastoreUtil.getCubeColTagKey(name), props);
  }

  public String getName() {
    return name;
  }

  /**
   * @return the startTime
   */
  public Date getStartTime() {
    return startTime;
  }

  /**
   * @return the endTime
   */
  public Date getEndTime() {
    return endTime;
  }

  public Optional<Long> getStartTimeMillisSinceEpoch() {

    if (startTime != null) {
      return Optional.of(startTime.getTime());
    }
    return Optional.absent();
  }

  public Optional<Long> getEndTimeMillisSinceEpoch() {

    if (endTime != null) {
      return Optional.of(endTime.getTime());
    }
    return Optional.absent();
  }

  public boolean isColumnAvailableInTimeRange(final TimeRange range) {
    return isColumnAvailableFrom(range.getFromDate()) && isColumnAvailableTill(range.getToDate());
  }

  public boolean isColumnAvailableFrom(@NonNull final Date date) {
    return (getStartTime() == null) ? true : date.equals(getStartTime()) || date.after(getStartTime());
  }

  public boolean isColumnAvailableTill(@NonNull final Date date) {
    return (getEndTime() == null) ? true : date.equals(getEndTime()) || date.before(getEndTime());
  }
  /**
   * @return the cost
   */
  public Double getCost() {
    return cost;
  }

  /**
   * @return the description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @return the displayString
   */
  public String getDisplayString() {
    return displayString;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(name);
    if (description != null) {
      builder.append(":");
      builder.append(description);
    }
    if (displayString != null) {
      builder.append(":");
      builder.append(displayString);
    }
    if (startTime != null) {
      builder.append("#start:");
      builder.append(COLUMN_TIME_FORMAT.get().format(startTime));
    }
    if (endTime != null) {
      builder.append("#end:");
      builder.append(COLUMN_TIME_FORMAT.get().format(endTime));
    }
    if (cost != null) {
      builder.append(":");
      builder.append(cost);
    }
    return builder.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((getName() == null) ? 0 : getName().toLowerCase().hashCode());
    result = prime * result + ((getDescription() == null) ? 0 : getDescription().toLowerCase().hashCode());
    result = prime * result + ((getDisplayString() == null) ? 0 : getDisplayString().toLowerCase().hashCode());
    result = prime * result + ((getName() == null) ? 0 : getName().toLowerCase().hashCode());
    result = prime * result + ((getStartTime() == null) ? 0 : COLUMN_TIME_FORMAT.get().format(
      getStartTime()).hashCode());
    result = prime * result + ((getEndTime() == null) ? 0 : COLUMN_TIME_FORMAT.get().format(getEndTime()).hashCode());
    result = prime * result + ((getCost() == null) ? 0 : getCost().hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    CubeColumn other = (CubeColumn) obj;
    if (this.getName() == null) {
      if (other.getName() != null) {
        return false;
      }
    } else if (!this.getName().equalsIgnoreCase(other.getName())) {
      return false;
    }

    if (this.getDescription() == null) {
      if (other.getDescription() != null) {
        return false;
      }
    } else if (!this.getDescription().equalsIgnoreCase(other.getDescription())) {
      return false;
    }

    if (this.getDisplayString() == null) {
      if (other.getDisplayString() != null) {
        return false;
      }
    } else if (!this.getDisplayString().equals(other.getDisplayString())) {
      return false;
    }

    if (this.getStartTime() == null) {
      if (other.getStartTime() != null) {
        return false;
      }
    } else if (other.getStartTime() == null) {
      return false;
    } else if (!COLUMN_TIME_FORMAT.get().format(this.getStartTime()).equals(COLUMN_TIME_FORMAT.get().format(
      other.getStartTime()))) {
      return false;
    }

    if (this.getEndTime() == null) {
      if (other.getEndTime() != null) {
        return false;
      }
    } else if (other.getEndTime() == null) {
      return false;
    } else if (!COLUMN_TIME_FORMAT.get().format(this.getEndTime()).equals(COLUMN_TIME_FORMAT.get().format(
      other.getEndTime()))) {
      return false;
    }

    if (this.getCost() == null) {
      if (other.getCost() != null) {
        return false;
      }
    } else if (!this.getCost().equals(other.getCost())) {
      return false;
    }
    return true;
  }

  public void addProperties(Map<String, String> props) {
    if (description != null) {
      props.put(MetastoreUtil.getCubeColDescriptionKey(getName()), description);
    }
    if (displayString != null) {
      props.put(MetastoreUtil.getCubeColDisplayKey(getName()), displayString);
    }
    if (startTime != null) {
      props.put(MetastoreUtil.getCubeColStartTimePropertyKey(getName()), COLUMN_TIME_FORMAT.get().format(startTime));
    }
    if (endTime != null) {
      props.put(MetastoreUtil.getCubeColEndTimePropertyKey(getName()), COLUMN_TIME_FORMAT.get().format(endTime));
    }
    if (cost != null) {
      props.put(MetastoreUtil.getCubeColCostPropertyKey(getName()), cost.toString());
    }
    if (tags != null) {
      addTagProperties(name, props, tags);
    }
  }
}
