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

package org.apache.lens.regression.util;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HadoopUtil {

  private HadoopUtil() {

  }

  static String rmIp = Util.getProperty("lens.remote.host");

  public static Configuration getHadoopConfiguration() {
    Configuration conf = new Configuration();
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    conf.set("fs.default.name", Util.getProperty("lens.server.hdfsurl"));
    return conf;
  }

  public static void uploadJars(String sourcePath, String hdfsDestinationPath) throws IOException {

    Configuration conf = HadoopUtil.getHadoopConfiguration();
    FileSystem fs = FileSystem.get(conf);

    Path localFilePath = new Path(sourcePath);
    Path hdfsFilePath = new Path(hdfsDestinationPath);

    log.info("Copying " + sourcePath + " to " + hdfsDestinationPath);
    fs.copyFromLocalFile(localFilePath, hdfsFilePath);
    log.info("Copied Successfully " + sourcePath + " to " + hdfsDestinationPath);

  }

}
