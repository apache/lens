package org.apache.lens.api.query;

/*
 * #%L
 * Grill API
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.Priority;


import lombok.*;

@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class LensQuery {

  @XmlElement @Getter private QueryHandle queryHandle;
  @XmlElement @Getter private String userQuery;
  @XmlElement @Getter private String submittedUser;
  @XmlElement @Getter private Priority priority;
  @XmlElement @Getter private boolean isPersistent;
  @XmlElement @Getter private String selectedDriverClassName;
  @XmlElement @Getter private String driverQuery;
  @XmlElement @Getter private QueryStatus status;
  @XmlElement @Getter private String resultSetPath;
  @XmlElement @Getter private String driverOpHandle;
  @XmlElement @Getter private LensConf queryConf;
  @XmlElement @Getter private long submissionTime;
  @XmlElement @Getter private long launchTime;
  @XmlElement @Getter private long driverStartTime;
  @XmlElement @Getter private long driverFinishTime;
  @XmlElement @Getter private long finishTime;
  @XmlElement @Getter private long closedTime;
  @XmlElement @Getter private String queryName;

}