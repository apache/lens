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
package org.apache.lens.examples;


import static org.apache.lens.api.jaxb.LensJAXBContext.unmarshallFromFile;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.lens.api.jaxb.YAMLToStringStrategyTest;

import com.beust.jcommander.internal.Lists;

public class ExampleSchemaToStringTest extends YAMLToStringStrategyTest {

  public List<ToStringTestData> provideData() throws URISyntaxException, JAXBException, IOException {
    List<ToStringTestData> ret = Lists.newArrayList();
    for (String yamlName : new File(getClass().getResource("/yaml").toURI()).list()) {
      ret.add(new ToStringTestData(yamlName, unmarshallFromFile("/" + yamlName.replaceAll("yaml$", "xml")),
        readYAML("/yaml/" + yamlName)));
    }
    return ret;
  }
}
