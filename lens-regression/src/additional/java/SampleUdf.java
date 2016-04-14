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

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public final class SampleUdf extends UDF {
  public Text evaluate(final Text s, Text sleepTime) throws InterruptedException {

    Long time = 180*1000L;

    if(sleepTime != null){
      time = Long.parseLong(sleepTime.toString()) * 1000L;
    }

    System.out.println("Sleep Time : " + time);

    Thread.sleep(time);

    if (s == null) { return null; }

    return new Text(s.toString().toLowerCase());
  }
}