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
package org.apache.lens.api.jaxb;

import java.io.*;
import java.util.Collection;
import java.util.List;
import java.util.TimeZone;

import org.apache.lens.api.ToXMLString;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import lombok.Data;
import lombok.RequiredArgsConstructor;

public class YAMLToStringStrategyTest {
  String timeZone;
  public static final String XML_LICENSE_HEADER = "<!--\n"
    + "\n"
    + "  Licensed to the Apache Software Foundation (ASF) under one\n"
    + "  or more contributor license agreements. See the NOTICE file\n"
    + "  distributed with this work for additional information\n"
    + "  regarding copyright ownership. The ASF licenses this file\n"
    + "  to you under the Apache License, Version 2.0 (the\n"
    + "  \"License\"); you may not use this file except in compliance\n"
    + "  with the License. You may obtain a copy of the License at\n"
    + "\n"
    + "  http://www.apache.org/licenses/LICENSE-2.0\n"
    + "\n"
    + "  Unless required by applicable law or agreed to in writing,\n"
    + "  software distributed under the License is distributed on an\n"
    + "  \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\n"
    + "  KIND, either express or implied. See the License for the\n"
    + "  specific language governing permissions and limitations\n"
    + "  under the License.\n"
    + "\n"
    + "-->\n";

  public static final String YAML_HEADER = "# Licensed to the Apache Software Foundation (ASF) under one\n"
    + "# or more contributor license agreements.  See the NOTICE file\n"
    + "# distributed with this work for additional information\n"
    + "# regarding copyright ownership.  The ASF licenses this file\n"
    + "# to you under the Apache License, Version 2.0 (the\n"
    + "# \"License\"); you may not use this file except in compliance\n"
    + "# with the License.  You may obtain a copy of the License at\n"
    + "#\n"
    + "#    http://www.apache.org/licenses/LICENSE-2.0\n"
    + "#\n"
    + "# Unless required by applicable law or agreed to in writing, software\n"
    + "# distributed under the License is distributed on an \"AS IS\" BASIS,\n"
    + "# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"
    + "# See the License for the specific language governing permissions and\n"
    + "# limitations under the License.\n";

  @BeforeClass
  public void setTimeZone() throws IOException {
    timeZone = System.getProperty("user.timezone");
    System.setProperty("user.timezone", "GMT");
    TimeZone.setDefault(null);
    write();
  }

  private void write() throws IOException {
    for (Object object : getNewObjectsForYAML()) {
      BufferedWriter xmlWriter = new BufferedWriter(new FileWriter(new File("src/test/resources/toString/"
        + object.getClass().getName() + ".xml")));
      BufferedWriter toStringWriter = new BufferedWriter(new FileWriter(new File("src/test/resources/toString/"
        + object.getClass().getName() + ".yaml")));
      xmlWriter.write(XML_LICENSE_HEADER);
      xmlWriter.write(ToXMLString.toString(object));
      xmlWriter.close();

      toStringWriter.write(YAML_HEADER);
      toStringWriter.write(object.toString());
      toStringWriter.close();
    }
  }

  private Collection<Object> getNewObjectsForYAML() {
    return Lists.newArrayList();
  }

  @AfterClass
  public void reSetTimeZone() {
    System.setProperty("user.timezone", timeZone);
    TimeZone.setDefault(null);
  }

  @Data
  @RequiredArgsConstructor
  public static class ToStringTestData {
    private final String name;
    private final Object object;
    private final String toString;
    private Throwable throwable;

    public ToStringTestData(String name, Throwable th) {
      this(name, null, null);
      this.throwable = th;
    }
    public void verify() throws Throwable {
      if (this.throwable != null) {
        throw throwable;
      }
      Assert.assertEquals(object.toString().trim(), toString.trim(), "toString didn't match for " + name);
    }
  }

  @DataProvider
  public Object[][] toStringDataProvider() throws Exception {
    Collection<ToStringTestData> dataCollection = provideData();
    Object[][] array = new Object[dataCollection.size()][1];
    int i = 0;
    for (ToStringTestData data : dataCollection) {
      array[i++][0] = data;
    }
    return array;
  }

  protected Collection<ToStringTestData> provideData() throws Exception {
    List<ToStringTestData> dataList = Lists.newArrayList();
    for (String fn : new File(getClass().getResource("/toString").toURI()).list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith("xml");
      }
    })) {
      Class<?> clazz = Class.forName(fn.substring(0, fn.length() - 4));
      Object unmarshalled = clazz.cast(new LensJAXBContext(clazz)
        .createUnmarshaller().unmarshal(getClass().getResourceAsStream("/toString/" + fn)));
      String toString = readYAML("/toString/" + fn.replaceAll("xml$", "yaml"));
      dataList.add(new ToStringTestData(fn, unmarshalled, toString));
    }
    return dataList;
  }

  @Test(dataProvider = "toStringDataProvider")
  public void testToString(ToStringTestData testData) throws Throwable {
    testData.verify();
  }

  protected String readYAML(String s) throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(s)));
    String line = br.readLine();
    StringBuilder sb = new StringBuilder();
    while (line != null) {
      if (!line.startsWith("#")) {
        sb.append(line).append("\n");
      }
      line = br.readLine();
    }
    return sb.toString().trim();
  }
}
