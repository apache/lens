/*
 * #%L
 * Lens ML Lib
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
package org.apache.lens.api.ml;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor
public class ModelMetadata {
  @XmlElement @Getter
  private String modelID;

  @XmlElement @Getter
  private String table;

  @XmlElement @Getter
  private String algorithm;

  @XmlElement @Getter
  private String params;

  @XmlElement @Getter
  private String createdAt;

  @XmlElement @Getter
  private String modelPath;

  @XmlElement @Getter
  private String labelColumn;

  @XmlElement @Getter
  private String features;

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();

    builder.append("Algorithm: ").append(algorithm).append('\n');
    builder.append("Model ID: ").append(modelID).append('\n');
    builder.append("Training table: ").append(table).append('\n');
    builder.append("Features: ").append(features).append('\n');
    builder.append("Labelled Column: ").append(labelColumn).append('\n');
    builder.append("Training params: ").append(params).append('\n');
    builder.append("Created on: ").append(createdAt).append('\n');
    builder.append("Model saved at: ").append(modelPath).append('\n');
    return builder.toString();
  }
}
