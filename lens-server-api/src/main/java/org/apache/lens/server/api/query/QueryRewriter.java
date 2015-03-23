
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
package org.apache.lens.server.api.query;

import org.apache.lens.api.LensException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * The Interface QueryRewriter.
 */
public interface QueryRewriter {

  /**
   * Rewrite.
   * @param query     the query
   * @param queryConf the query configuration
   * @param metastoreConf The metastore configuration. If rewriters requires to access metastore, this configuration
   *  needs to passed
   *
   * @return the string
   * @throws LensException the lens exception
   */
  String rewrite(String query, Configuration queryConf, HiveConf metastoreConf) throws LensException;

  /**
   * Set conf for the rewriter
   *
   * @param rewriteConf Configuration required for rewriter init
   */
  void init(Configuration rewriteConf);
}
