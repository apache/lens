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
package org.apache.lens.rdd;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * The Class HCatRecordToObjectListMapper.
 */
public class HCatRecordToObjectListMapper implements Function<Tuple2<WritableComparable, HCatRecord>, List<Object>> {

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
   */
  @Override
  public List<Object> call(Tuple2<WritableComparable, HCatRecord> hcatTuple) throws Exception {
    HCatRecord record = hcatTuple._2();

    if (record == null) {
      return null;
    }

    List<Object> row = new ArrayList<Object>(record.size());
    for (int i = 0; i < record.size(); i++) {
      row.add(record.get(i));
    }
    return row;
  }
}
