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
package org.apache.lens.lib.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.io.Text;
import org.apache.lens.api.query.ResultRow;
import org.apache.lens.lib.query.FileSerdeFormatter;
import org.apache.lens.lib.query.WrappedFileFormatter;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.query.InMemoryOutputFormatter;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * The Class TestFileSerdeFormatter.
 */
public class TestFileSerdeFormatter extends TestAbstractFileFormatter {

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.lib.query.TestAbstractFileFormatter#testFormatter()
   */
  @Test
  public void testFormatter() throws IOException {
    super.testFormatter();
    validateSerde(LensConfConstants.DEFAULT_OUTPUT_SERDE, Text.class.getCanonicalName());
  }

  /**
   * Test serde.
   *
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  @Test
  public void testSerde() throws IOException {
    Configuration conf = new Configuration();
    conf.set(LensConfConstants.QUERY_OUTPUT_FILE_EXTN, ".txt");
    conf.set(LensConfConstants.QUERY_OUTPUT_SERDE, LazySimpleSerDe.class.getCanonicalName());
    testFormatter(conf, "UTF8", LensConfConstants.RESULT_SET_PARENT_DIR_DEFAULT, ".txt",
        getMockedResultSetWithoutComma());
    validateSerde(LazySimpleSerDe.class.getCanonicalName(), Text.class.getCanonicalName());

    // validate rows
    Assert.assertEquals(readFinalOutputFile(new Path(formatter.getFinalOutputPath()), conf, "UTF-8"),
        getExpectedTextRowsWithoutComma());
  }

  /**
   * Test compression with custom serde.
   *
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  @Test
  public void testCompressionWithCustomSerde() throws IOException {
    Configuration conf = new Configuration();
    conf.set(LensConfConstants.QUERY_OUTPUT_FILE_EXTN, ".txt");
    conf.set(LensConfConstants.QUERY_OUTPUT_SERDE, LazySimpleSerDe.class.getCanonicalName());
    conf.setBoolean(LensConfConstants.QUERY_OUTPUT_ENABLE_COMPRESSION, true);
    testFormatter(conf, "UTF8", LensConfConstants.RESULT_SET_PARENT_DIR_DEFAULT, ".txt.gz",
        getMockedResultSetWithoutComma());
    validateSerde(LazySimpleSerDe.class.getCanonicalName(), Text.class.getCanonicalName());
    // validate rows
    Assert.assertEquals(readCompressedFile(new Path(formatter.getFinalOutputPath()), conf, "UTF-8"),
        getExpectedTextRowsWithoutComma());
  }

  /**
   * Test text file with zip formatter.
   *
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  @Test
  public void testTextFileWithZipFormatter() throws IOException {
    Configuration conf = new Configuration();
    setConf(conf);
    conf.set(LensConfConstants.QUERY_OUTPUT_FILE_EXTN, ".txt");
    conf.set(LensConfConstants.QUERY_OUTPUT_SERDE, LazySimpleSerDe.class.getCanonicalName());
    conf.setBoolean(LensConfConstants.RESULT_SPLIT_INTO_MULTIPLE, true);
    conf.setLong(LensConfConstants.RESULT_SPLIT_MULTIPLE_MAX_ROWS, 2L);
    testFormatter(conf, "UTF8", LensConfConstants.RESULT_SET_PARENT_DIR_DEFAULT, ".zip",
        getMockedResultSetWithoutComma());
    // validate rows
    List<String> actual = readZipOutputFile(new Path(formatter.getFinalOutputPath()), conf, "UTF-8");
    Assert.assertEquals(actual, getExpectedTextRowsWithMultipleWithoutComma());
  }

  /**
   * Test csv with zip formatter.
   *
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  @Test
  public void testCSVWithZipFormatter() throws IOException {
    Configuration conf = new Configuration();
    setConf(conf);
    conf.setBoolean(LensConfConstants.RESULT_SPLIT_INTO_MULTIPLE, true);
    conf.setLong(LensConfConstants.RESULT_SPLIT_MULTIPLE_MAX_ROWS, 2L);
    testFormatter(conf, "UTF8", LensConfConstants.RESULT_SET_PARENT_DIR_DEFAULT, ".zip", getMockedResultSet());
    // validate rows
    List<String> actual = readZipOutputFile(new Path(formatter.getFinalOutputPath()), conf, "UTF-8");
    Assert.assertEquals(actual, getExpectedCSVRowsWithMultiple());
  }

  /**
   * Validate serde.
   *
   * @param serdeClassName
   *          the serde class name
   * @param serializedClassName
   *          the serialized class name
   */
  private void validateSerde(String serdeClassName, String serializedClassName) {
    // check serde
    SerDe outputSerde = ((FileSerdeFormatter) formatter).getSerde();
    Assert.assertEquals(serdeClassName, outputSerde.getClass().getCanonicalName());
    Assert.assertEquals(serializedClassName, outputSerde.getSerializedClass().getCanonicalName());

  }

  private List<ResultRow> getTestRows() {
    List<ResultRow> rows = new ArrayList<ResultRow>();
    List<Object> elements = new ArrayList<Object>();
    Map<Integer, String> mapElements = new LinkedHashMap<Integer, String>();
    mapElements.put(1, "one");
    elements.add(1);
    elements.add("one");
    elements.add("one");
    elements.add("one");
    elements.add(Arrays.asList(new Byte((byte) 1)));
    elements.add(Arrays.asList(1, "one"));
    elements.add(mapElements);
    rows.add(new ResultRow(elements));

    mapElements = new LinkedHashMap<Integer, String>();
    mapElements.put(1, "one");
    mapElements.put(2, "two");
    elements = new ArrayList<Object>();
    elements.add(2);
    elements.add("two");
    elements.add("two");
    elements.add("two");
    elements.add(Arrays.asList(new Byte((byte) 1), new Byte((byte) 2)));
    elements.add(Arrays.asList(2, "two"));
    elements.add(mapElements);
    rows.add(new ResultRow(elements));

    mapElements = new LinkedHashMap<Integer, String>();
    mapElements.put(1, "one");
    mapElements.put(2, "two");
    mapElements.put(null, "three");
    elements = new ArrayList<Object>();
    elements.add(null);
    elements.add("three");
    elements.add("three");
    elements.add("three");
    elements.add(Arrays.asList(new Byte((byte) 1), new Byte((byte) 2), null));
    elements.add(Arrays.asList(null, "three"));
    elements.add(mapElements);
    rows.add(new ResultRow(elements));

    mapElements = new LinkedHashMap<Integer, String>();
    mapElements.put(1, "one");
    mapElements.put(2, "two");
    mapElements.put(null, "three");
    mapElements.put(4, null);
    elements = new ArrayList<Object>();
    elements.add(4);
    elements.add(null);
    elements.add(null);
    elements.add(null);
    elements.add(Arrays.asList(new Byte((byte) 1), new Byte((byte) 2), null, new Byte((byte) 4)));
    elements.add(Arrays.asList(4, null));
    elements.add(mapElements);
    rows.add(new ResultRow(elements));

    mapElements = new LinkedHashMap<Integer, String>();
    mapElements.put(1, "one");
    mapElements.put(2, "two");
    mapElements.put(null, "three");
    mapElements.put(4, null);
    mapElements.put(5, null);
    elements = new ArrayList<Object>();
    elements.add(null);
    elements.add(null);
    elements.add(null);
    elements.add(null);
    elements.add(Arrays.asList(new Byte((byte) 1), new Byte((byte) 2), null, new Byte((byte) 4), null));
    elements.add(Arrays.asList(null, null));
    elements.add(mapElements);
    rows.add(new ResultRow(elements));

    return rows;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.lib.query.TestAbstractFileFormatter#createFormatter()
   */
  @Override
  protected WrappedFileFormatter createFormatter() {
    return new FileSerdeFormatter();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.lib.query.TestAbstractFileFormatter#writeAllRows(org.apache.hadoop.conf.Configuration)
   */
  @Override
  protected void writeAllRows(Configuration conf) throws IOException {
    for (ResultRow row : getTestRows()) {
      ((InMemoryOutputFormatter) formatter).writeRow(row);
    }
  }

}
