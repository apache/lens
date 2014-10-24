package org.apache.lens.api.query;

/*
 * #%L
 * Lens API
 * %%
 * Copyright (C) 2014 Apache Software Foundation
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

import java.util.Date;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.lens.api.LensConf;


import lombok.AccessLevel;
import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class LensPreparedQuery {
  @XmlElement @Getter private QueryPrepareHandle prepareHandle;
  @XmlElement @Getter private String userQuery;
  @XmlElement @Getter private Date preparedTime;
  @XmlElement @Getter private String preparedUser;
  @XmlElement @Getter private String selectedDriverClassName;
  @XmlElement @Getter private String driverQuery;
  @XmlElement @Getter private LensConf conf;
}
