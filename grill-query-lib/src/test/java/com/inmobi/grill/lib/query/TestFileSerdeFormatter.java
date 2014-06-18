package com.inmobi.grill.lib.query;

/*
 * #%L
 * Grill Query Library
 * %%
 * Copyright (C) 2014 Inmobi
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.io.Text;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.inmobi.grill.api.query.ResultRow;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.query.InMemoryOutputFormatter;

public class TestFileSerdeFormatter extends TestAbstractFileFormatter {

  @Test
  public void testFormatter() throws IOException {
    super.testFormatter();
    validateSerde(GrillConfConstants.DEFAULT_OUTPUT_SERDE, Text.class.getCanonicalName());
  }

  @Test
  public void testSerde() throws IOException {
    Configuration conf = new Configuration();
    conf.set(GrillConfConstants.QUERY_OUTPUT_FILE_EXTN, ".txt");
    conf.set(GrillConfConstants.QUERY_OUTPUT_SERDE, LazySimpleSerDe.class.getCanonicalName());
    testFormatter(conf, "UTF8",
        GrillConfConstants.GRILL_RESULT_SET_PARENT_DIR_DEFAULT, ".txt");
    validateSerde(LazySimpleSerDe.class.getCanonicalName(),
        Text.class.getCanonicalName());

    // validate rows
    Assert.assertEquals(readFinalOutputFile(
        new Path(formatter.getFinalOutputPath()), conf, "UTF-8"), getExpectedTextRows());
  }

  @Test
  public void testCompressionWithCustomSerde() throws IOException {
    Configuration conf = new Configuration();
    conf.set(GrillConfConstants.QUERY_OUTPUT_FILE_EXTN, ".txt");
    conf.set(GrillConfConstants.QUERY_OUTPUT_SERDE, LazySimpleSerDe.class.getCanonicalName());
    conf.setBoolean(GrillConfConstants.QUERY_OUTPUT_ENABLE_COMPRESSION, true);
    testFormatter(conf, "UTF8",
        GrillConfConstants.GRILL_RESULT_SET_PARENT_DIR_DEFAULT, ".txt.gz");
    validateSerde(LazySimpleSerDe.class.getCanonicalName(),
        Text.class.getCanonicalName());
    // validate rows
    Assert.assertEquals(readCompressedFile(
        new Path(formatter.getFinalOutputPath()), conf, "UTF-8"), getExpectedTextRows());
  }

  private void validateSerde(String serdeClassName, String serializedClassName) {
    // check serde
    SerDe outputSerde = ((FileSerdeFormatter)formatter).getSerde();
    Assert.assertEquals(serdeClassName,
        outputSerde.getClass().getCanonicalName());
    Assert.assertEquals(serializedClassName,
        outputSerde.getSerializedClass().getCanonicalName());

  }

  private List<ResultRow> getTestRows() {
    List<ResultRow> rows = new ArrayList<ResultRow>();
    List<Object> elements = new ArrayList<Object>();
    elements.add(1);
    elements.add("one");
    rows.add(new ResultRow(elements));

    elements = new ArrayList<Object>();
    elements.add(2);
    elements.add("two");
    rows.add(new ResultRow(elements));

    elements = new ArrayList<Object>();
    elements.add(null);
    elements.add("three");
    rows.add(new ResultRow(elements));

    elements = new ArrayList<Object>();
    elements.add(4);
    elements.add(null);
    rows.add(new ResultRow(elements));

    elements = new ArrayList<Object>();
    elements.add(null);
    elements.add(null);
    rows.add(new ResultRow(elements));

    return rows;
  }
  
  @Override
  protected FileFormatter createFormatter() {
    return new FileSerdeFormatter();
  }

  @Override
  protected void writeAllRows(Configuration conf) throws IOException {
    for (ResultRow row : getTestRows()) {
      ((InMemoryOutputFormatter)formatter).writeRow(row);
    }    
  }

}
