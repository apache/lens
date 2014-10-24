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
package org.apache.lens.ml;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

@NoArgsConstructor
public abstract class MLModel<PREDICTION> implements Serializable {
  @Getter
  @Setter
  private String id;

  @Getter
  @Setter
  private Date createdAt;

  @Getter
  @Setter
  private String trainerName;

  @Getter
  @Setter
  private String table;

  @Getter
  @Setter
  private List<String> params;

  @Getter
  @Setter
  private String labelColumn;

  @Getter
  @Setter
  private List<String> featureColumns;

  public abstract PREDICTION predict(Object ... args);
}
