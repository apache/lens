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
import java.util.*;

import org.apache.lens.api.query.ResultRow;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.query.InMemoryOutputFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.io.Text;

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
   * @throws IOException Signals that an I/O exception has occurred.
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
   * @throws IOException Signals that an I/O exception has occurred.
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
   * @throws IOException Signals that an I/O exception has occurred.
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
   * @throws IOException Signals that an I/O exception has occurred.
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
   * @param serdeClassName      the serde class name
   * @param serializedClassName the serialized class name
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
    mapElements.put(2, "two, 3=three");
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

  protected List<String> getExpectedCSVRows() {
    return new ArrayList<String>() {
      {
        add("\"firstcol\",\"format(secondcol,2)\",\"thirdcol\",\"fourthcol\",\"fifthcol\",\"sixthcol\",\"seventhcol\"");
        add("\"1\",\"one\",\"one\",\"one\",\"[1]\",\"[1, one]\",\"{1=one}\"");
        add("\"2\",\"two\",\"two\",\"two\",\"[1, 2]\",\"[2, two]\",\"{1=one, 2=two, 3=three}\"");
        add("\"NULL\",\"three\",\"three\",\"three\",\"[1, 2, null]\",\"[null, three]\",\"{1=one, 2=two, null=three}\"");
        add("\"4\",\"NULL\",\"NULL\",\"NULL\",\"[1, 2, null, 4]\",\"[4, null]\","
          + "\"{1=one, 2=two, null=three, 4=null}\"");
        add("\"NULL\",\"NULL\",\"NULL\",\"NULL\",\"[1, 2, null, 4, null]\",\"[null, null]\","
          + "\"{1=one, 2=two, null=three, 4=null, 5=null}\"");
        add("Total rows:5");
      }
    };
  }

  protected List<String> getExpectedTextRows() {
    List<String> txtRows = new ArrayList<String>();
    txtRows.add("firstcolformat(secondcol,2)thirdcolfourthcolfifthcolsixthcolseventhcol");
    txtRows.add("1oneoneone            11one1one       ");
    txtRows.add("2twotwotwo            122two1one       2two       ");
    txtRows.add("\\Nthreethreethree          12\\N\\Nthree1one       2two       \\Nthree     ");
    txtRows.add("4\\N\\N\\N12\\N44\\N1one       2two       \\Nthree     4\\N");
    txtRows.add("\\N\\N\\N\\N12\\N4\\N\\N\\N1one       2two       \\Nthree     4\\N5\\N");
    txtRows.add("Total rows:5");
    return txtRows;
  }

  protected List<String> getExpectedCSVRowsWithoutComma() {
    List<String> csvRows = new ArrayList<String>();
    csvRows.add("\"firstcol\",\"secondcol\",\"thirdcol\",\"fourthcol\",\"fifthcol\",\"sixthcol\",\"seventhcol\"");
    csvRows.add("\"1\",\"one\",\"one\",\"one\",\"1\",\"1:one\",\"1=one\"");
    csvRows.add("\"2\",\"two\",\"two\",\"two\",\"1,2\",\"2:two\",\"1=one,2=two\"");
    csvRows.add("\"NULL\",\"three\",\"three\",\"three\",\"1,2,NULL\",\"NULL:three\",\"1=one,2=two,NULL=three\"");
    csvRows.add("\"4\",\"NULL\",\"NULL\",\"NULL\",\"1,2,NULL,4\",\"4:NULL\",\"1=one,2=two,NULL=three,4=NULL\"");
    csvRows
      .add("\"NULL\",\"NULL\",\"NULL\",\"NULL\",\"1,2,NULL,4,NULL\",\"NULL:NULL\","
        + "\"1=one,2=two,NULL=three,4=NULL,5=NULL\"");
    csvRows.add("Total rows:5");
    return csvRows;
  }

  protected List<String> getExpectedTextRowsWithoutComma() {
    return new ArrayList<String>() {
      {
        add("firstcol\u0001secondcol\u0001thirdcol\u0001fourthcol\u0001fifthcol\u0001sixthcol\u0001seventhcol");
        add("1\u0001one\u0001one\u0001one            \u0001[1]\u0001[1, one]\u0001{1=one}");
        add("2\u0001two\u0001two\u0001two            \u0001[1, 2]\u0001[2, two]\u0001{1=one, 2=two, 3=three}");
        add("\\N\u0001three\u0001three\u0001three          \u0001[1, 2, null]\u0001"
          + "[null, three]\u0001{1=one, 2=two, null=three}");
        add("4\u0001\\N\u0001\\N\u0001\\N\u0001[1, 2, null, 4]\u0001[4, null]\u0001{1=one, 2=two, null=three, 4=null}");
        add("\\N\u0001\\N\u0001\\N\u0001\\N\u0001[1, 2, null, 4, null]\u0001[null, null]"
          + "\u0001{1=one, 2=two, null=three, 4=null, 5=null}");
        add("Total rows:5");
      }
    };
  }

  protected List<String> getExpectedCSVRowsWithMultiple() {
    return new ArrayList<String>() {
      {
        add("\"firstcol\",\"format(secondcol,2)\",\"thirdcol\",\"fourthcol\",\"fifthcol\",\"sixthcol\",\"seventhcol\"");
        add("\"1\",\"one\",\"one\",\"one\",\"[1]\",\"[1, one]\",\"{1=one}\"");
        add("\"2\",\"two\",\"two\",\"two\",\"[1, 2]\",\"[2, two]\",\"{1=one, 2=two, 3=three}\"");
        add("\"firstcol\",\"format(secondcol,2)\",\"thirdcol\",\"fourthcol\",\"fifthcol\",\"sixthcol\",\"seventhcol\"");
        add("\"NULL\",\"three\",\"three\",\"three\",\"[1, 2, null]\",\"[null, three]\",\"{1=one, 2=two, null=three}\"");
        add("\"4\",\"NULL\",\"NULL\",\"NULL\",\"[1, 2, null, 4]\",\"[4, null]\","
          + "\"{1=one, 2=two, null=three, 4=null}\"");
        add("\"firstcol\",\"format(secondcol,2)\",\"thirdcol\",\"fourthcol\",\"fifthcol\",\"sixthcol\",\"seventhcol\"");
        add("\"NULL\",\"NULL\",\"NULL\",\"NULL\",\"[1, 2, null, 4, null]\","
          + "\"[null, null]\",\"{1=one, 2=two, null=three, 4=null, 5=null}\"");
        add("Total rows:5");
      }
    };
  }

  protected List<String> getExpectedTextRowsWithMultiple() {
    List<String> txtRows = new ArrayList<String>();
    txtRows.add("firstcolformat(secondcol,2)thirdcolfourthcolfifthcolsixthcolseventhcol");
    txtRows.add("1oneoneone            11one1one       ");
    txtRows.add("2twotwotwo            122two1one       2two       ");
    txtRows.add("firstcolformat(secondcol,2)thirdcolfourthcolfifthcolsixthcolseventhcol");
    txtRows.add("\\Nthreethreethree          12\\N\\Nthree1one       2two       \\Nthree     ");
    txtRows.add("4\\N\\N\\N12\\N44\\N1one       2two       \\Nthree     4\\N");
    txtRows.add("firstcolformat(secondcol,2)thirdcolfourthcolfifthcolsixthcolseventhcol");
    txtRows.add("\\N\\N\\N\\N12\\N4\\N\\N\\N1one       2two       \\Nthree     4\\N5\\N");
    txtRows.add("Total rows:5");
    return txtRows;
  }

  protected List<String> getExpectedCSVRowsWithMultipleWithoutComma() {
    List<String> csvRows = new ArrayList<String>();
    csvRows.add("\"firstcol\",\"secondcol\",\"thirdcol\",\"fourthcol\",\"fifthcol\",\"sixthcol\",\"seventhcol\"");
    csvRows.add("\"1\",\"one\",\"one\",\"one\",\"1\",\"1:one\",\"1=one\"");
    csvRows.add("\"2\",\"two\",\"two\",\"two\",\"1,2\",\"2:two\",\"1=one,2=two\"");
    csvRows.add("\"firstcol\",\"secondcol\",\"thirdcol\",\"fourthcol\",\"fifthcol\",\"sixthcol\",\"seventhcol\"");
    csvRows.add("\"NULL\",\"three\",\"three\",\"three\",\"1,2,NULL\",\"NULL:three\",\"1=one,2=two,NULL=three\"");
    csvRows.add("\"4\",\"NULL\",\"NULL\",\"NULL\",\"1,2,NULL,4\",\"4:NULL\",\"1=one,2=two,NULL=three,4=NULL\"");
    csvRows.add("\"firstcol\",\"secondcol\",\"thirdcol\",\"fourthcol\",\"fifthcol\",\"sixthcol\",\"seventhcol\"");
    csvRows
      .add("\"NULL\",\"NULL\",\"NULL\",\"NULL\",\"1,2,NULL,4,NULL\",\"NULL:NULL\","
        + "\"1=one,2=two,NULL=three,4=NULL,5=NULL\"");
    csvRows.add("Total rows:5");
    return csvRows;
  }

  protected List<String> getExpectedTextRowsWithMultipleWithoutComma() {
    return new ArrayList<String>() {
      {
        add("firstcol\u0001secondcol\u0001thirdcol\u0001fourthcol\u0001fifthcol\u0001sixthcol\u0001seventhcol");
        add("1\u0001one\u0001one\u0001one            \u0001[1]\u0001[1, one]\u0001{1=one}");
        add("2\u0001two\u0001two\u0001two            \u0001[1, 2]\u0001[2, two]\u0001{1=one, 2=two, 3=three}");
        add("firstcol\u0001secondcol\u0001thirdcol\u0001fourthcol\u0001fifthcol\u0001sixthcol\u0001seventhcol");
        add("\\N\u0001three\u0001three\u0001three          \u0001[1, 2, null]"
          + "\u0001[null, three]\u0001{1=one, 2=two, null=three}");
        add("4\u0001\\N\u0001\\N\u0001\\N\u0001[1, 2, null, 4]\u0001[4, null]\u0001{1=one, 2=two, null=three, 4=null}");
        add("firstcol\u0001secondcol\u0001thirdcol\u0001fourthcol\u0001fifthcol\u0001sixthcol\u0001seventhcol");
        add("\\N\u0001\\N\u0001\\N\u0001\\N\u0001[1, 2, null, 4, null]"
          + "\u0001[null, null]\u0001{1=one, 2=two, null=three, 4=null, 5=null}");
        add("Total rows:5");
      }
    };
  }

}
