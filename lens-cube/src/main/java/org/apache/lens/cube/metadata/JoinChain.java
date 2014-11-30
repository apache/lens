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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import org.apache.commons.lang.StringUtils;

@EqualsAndHashCode
@ToString
public class JoinChain implements Named {
  @Getter private final String name;
  @Getter private final String displayString;
  @Getter private final String description;
  // There can be more than one path associated with same name.
  // c1.r1->t1.k1
  // c1.r2->t1.k2
  @Getter private final List<List<TableReference>> paths = new ArrayList<List<TableReference>>();

  public void addProperties(Map<String, String> props) {
    props.put(MetastoreUtil.getJoinChainNumChainsKey(getName()), String.valueOf(paths.size()));
    for (int i = 0; i< paths.size(); i++) {
      props.put(MetastoreUtil.getJoinChainFullChainKey(getName(), i), MetastoreUtil.getReferencesString(paths.get(i)));
    }
    props.put(MetastoreUtil.getJoinChainDisplayKey(getName()), displayString);
    props.put(MetastoreUtil.getJoinChainDescriptionKey(getName()), description);
  }

  /**
   * Construct join chain
   *
   * @param name
   * @param display
   * @param description
   */
  public JoinChain(String name, String display, String description) {
    this.name = name.toLowerCase();
    this.displayString = display;
    this.description = description;
  }

  /**
   * This is used only for serializing
   *
   * @param name
   * @param props
   */
  public JoinChain(String name, Map<String, String> props) {
    this.name = name;
    int numChains = Integer.parseInt(props.get(MetastoreUtil.getJoinChainNumChainsKey(getName())));
    for (int i = 0; i < numChains; i++) {
      List<TableReference> chain = new ArrayList<TableReference>();
      String refListStr = props.get(MetastoreUtil.getJoinChainFullChainKey(getName(), i));
      String refListDims[] = StringUtils.split(refListStr, ",");
      for (String refDimRaw : refListDims) {
        chain.add(new TableReference(refDimRaw));
      }
      paths.add(chain);
    }
    this.description = props.get(MetastoreUtil.getJoinChainDescriptionKey(name));
    this.displayString = props.get(MetastoreUtil.getJoinChainDisplayKey(name));
  }

  public void addPath(List<TableReference> path) {
    if (path.size() <= 1) {
      throw new IllegalArgumentException("Path should contain atlease two links");
    }
    this.paths.add(path);
  }
}
