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
package org.apache.lens.ml.api;

import java.util.Date;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.lens.api.LensSessionHandle;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * The Model Instance Class. Contains meta data for TrainedModel i.e. information about the algorithm used for
 * training and specification about features and label. Model instance captures meta data for the process of training
 * Model modelId using test data from dataSet.
 */
@AllArgsConstructor
@XmlRootElement
public class ModelInstance implements MLProcess {

  @XmlElement
  @Getter
  @Setter
  String id;

  @XmlElement
  @Getter
  @Setter
  Date startTime;

  @XmlElement
  @Getter
  @Setter
  Date finishTime;

  @XmlElement
  @Getter
  @Setter
  Status status;

  @Getter
  @Setter
  @XmlElement
  LensSessionHandle lensSessionHandle;

  @Getter
  @Setter
  @XmlElement
  String modelId;

  @Getter
  @Setter
  @XmlElement
  String dataSet;

  @Getter
  @Setter
  @XmlElement
  String path;

  @Getter
  @Setter
  @XmlElement
  String evaluationId;

}
