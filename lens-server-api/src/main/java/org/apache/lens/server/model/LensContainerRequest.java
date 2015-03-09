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
package org.apache.lens.server.model;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.message.internal.ReaderWriter;
import org.glassfish.jersey.server.ContainerRequest;

import com.google.common.base.Optional;
import lombok.AllArgsConstructor;
import lombok.NonNull;

/**
 * Wrapper class for ContainerRequest.
 *
 * Has helper methods to give form data values for the request. More helper methods can be included.
 */
@AllArgsConstructor
public class LensContainerRequest {
  @NonNull
  private ContainerRequest containerRequest;

  /**
   * parses form data as multipart form data and extracts the value of given fieldName from it.
   *
   * @param fieldName
   * @return Optional value of field passed as multipart post data
   */
  public Optional<String> getFormDataFieldValue(final String fieldName) {
    FormDataBodyPart field = getFormData(FormDataMultiPart.class).getField(fieldName);
    return field == null ? Optional.<String>absent() : Optional.of(field.getValue());
  }

  /**
   * Utility method for reading form/multipart-form data from container request.
   *
   * @param clz Either Form.class or FormDataMultiPart.class
   * @param <T> clz type
   * @return an instance of T
   */
  public <T> T getFormData(Class<T> clz) {

    InputStream in = containerRequest.getEntityStream();
    if (in.getClass() != ByteArrayInputStream.class) {
      // Buffer input
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try {
        ReaderWriter.writeTo(in, baos);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
      in = new ByteArrayInputStream(baos.toByteArray());
      containerRequest.setEntityStream(in);
    }
    ByteArrayInputStream bais = (ByteArrayInputStream) in;
    T f = containerRequest.readEntity(clz);
    bais.reset();
    return f;
  }
}
