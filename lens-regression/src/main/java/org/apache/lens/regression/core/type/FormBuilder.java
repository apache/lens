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

package org.apache.lens.regression.core.type;

import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

import javax.ws.rs.core.MediaType;


import org.glassfish.jersey.media.multipart.FormDataMultiPart;


public class FormBuilder {
  private FormDataMultiPart formData;

  public FormBuilder() {
    formData = new FormDataMultiPart();
  }

  public FormBuilder(FormDataMultiPart form) {
    formData = new FormDataMultiPart();
    formData = form;
  }

  public FormBuilder(String fieldName, String value) {
    formData = new FormDataMultiPart();
    formData.field(fieldName, value);
  }

  public FormBuilder(Properties newProperty) {
    formData = new FormDataMultiPart();
    Enumeration<?> e = newProperty.propertyNames();
    while (e.hasMoreElements()) {
      String key = (String) e.nextElement();
      formData.field(key, newProperty.getProperty(key));
    }
  }

  public FormBuilder(Map<String, String> map) {
    formData = new FormDataMultiPart();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      formData.field(entry.getKey(), entry.getValue());
    }
  }

  public void add(String fieldName, String value) {
    formData.field(fieldName, value);
  }

  public void add(String fieldName, Object value, MediaType mediaType) {
    formData.field(fieldName, value, mediaType);
  }

  public void addFile(String fieldName, String filename) {
  }

  public FormDataMultiPart getForm() {
    return formData;
  }

}
