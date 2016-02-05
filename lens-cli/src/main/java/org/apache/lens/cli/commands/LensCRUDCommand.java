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
package org.apache.lens.cli.commands;

import java.io.File;
import java.util.List;

import org.apache.lens.api.APIResult;

import com.google.common.base.Joiner;

public abstract class LensCRUDCommand<T> extends BaseLensCommand {

  // Template methods. overriding classes need a binding to commands so
  // they can call them directly from their own methods.
  public String showAll() {
    List<String> all = getAll();
    if (all == null || all.isEmpty()) {
      return "No " + getSingleObjectName() + " found";
    }
    return Joiner.on("\n").join(all);
  }

  public String create(File path, boolean ignoreIfExists) {
    return doCreate(getValidPath(path, false, true), ignoreIfExists)
        .getStatus().toString().toLowerCase();
  }

  public String describe(String name) {
    return formatJson(doRead(name));
  }

  public String update(String entity, File path) {
    return doUpdate(entity, getValidPath(path, false, true))
        .getStatus().toString().toLowerCase();
  }

  public String drop(String name, boolean cascade) {
    return doDelete(name, cascade).getStatus().toString().toLowerCase();
  }

  public String getSingleObjectName() {
    return getClass().getSimpleName().substring(4, getClass().getSimpleName().indexOf("Command")).toLowerCase();
  }

  // Actual operations. Need to be overridden by overriding classes

  public abstract List<String> getAll();

  protected abstract APIResult doCreate(String path, boolean ignoreIfExists);

  protected abstract T doRead(String name);

  public abstract APIResult doUpdate(String name, String path);

  protected abstract APIResult doDelete(String name, boolean cascade);
}
