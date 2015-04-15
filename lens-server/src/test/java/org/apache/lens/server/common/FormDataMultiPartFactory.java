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

package org.apache.lens.server.common;

import javax.ws.rs.core.MediaType;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.metastore.ObjectFactory;
import org.apache.lens.api.metastore.XFactTable;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;

import com.google.common.base.Optional;

public class FormDataMultiPartFactory {

  private static ObjectFactory cubeObjectFactory = new ObjectFactory();

  protected FormDataMultiPartFactory() {
    throw new UnsupportedOperationException();
  }

  public static FormDataMultiPart createFormDataMultiPartForQuery(final Optional<LensSessionHandle> sessionId,
      final Optional<String> query, final Optional<String> operation, final LensConf lensConf) {

    final FormDataMultiPart mp = new FormDataMultiPart();

    if (sessionId.isPresent()) {
      mp.bodyPart(getSessionIdFormDataBodyPart(sessionId.get()));
    }

    if (query.isPresent()) {
      mp.bodyPart(getFormDataBodyPart("query", query.get()));
    }

    if (operation.isPresent()) {
      mp.bodyPart(getFormDataBodyPart("operation", operation.get()));
    }

    mp.bodyPart(getFormDataBodyPart("conf", "conf", lensConf));
    return mp;
  }

  public static FormDataMultiPart createFormDataMultiPartForSession(final Optional<LensSessionHandle> sessionId,
      final Optional<String> username, final Optional<String> password, final Optional<LensConf> lensConf) {

    final FormDataMultiPart mp = new FormDataMultiPart();

    if (sessionId.isPresent()) {
      mp.bodyPart(getSessionIdFormDataBodyPart(sessionId.get()));
    }

    if (username.isPresent()) {
      mp.bodyPart(getFormDataBodyPart("username", username.get()));
    }

    if (password.isPresent()) {
      mp.bodyPart(getFormDataBodyPart("password", password.get()));
    }

    if (lensConf.isPresent()) {
      mp.bodyPart(getFormDataBodyPart("sessionconf", "sessionconf", lensConf.get()));
    }

    return mp;
  }

  public static FormDataMultiPart createFormDataMultiPartForFact(final LensSessionHandle sessionId,
      final XFactTable xFactTable) {

    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(getSessionIdFormDataBodyPart(sessionId));
    mp.bodyPart(getFormDataBodyPart("fact", "fact", cubeObjectFactory.createXFactTable(xFactTable)));

    return mp;
  }

  private static FormDataBodyPart getFormDataBodyPart(final String fdContentDispName, final String value) {
    return new FormDataBodyPart(FormDataContentDisposition.name(fdContentDispName).build(), value,
        MediaType.APPLICATION_XML_TYPE);
  }

  private static FormDataBodyPart getFormDataBodyPart(final String fdContentDispName, final Object entity) {
    return new FormDataBodyPart(FormDataContentDisposition.name(fdContentDispName).build(), entity,
        MediaType.APPLICATION_XML_TYPE);
  }

  private static FormDataBodyPart getFormDataBodyPart(final String fdContentDispName, final String fileName,
      final Object entity) {
    return new FormDataBodyPart(FormDataContentDisposition.name(fdContentDispName).fileName(fileName).build(), entity,
        MediaType.APPLICATION_XML_TYPE);
  }

  private static FormDataBodyPart getFormDataBodyPartWithOutEntity(final String fdContentDispName,
      final String fileName) {
    return new FormDataBodyPart(FormDataContentDisposition.name(fdContentDispName).fileName(fileName).build(),
        MediaType.APPLICATION_XML_TYPE);
  }

  private static FormDataBodyPart getSessionIdFormDataBodyPart(final LensSessionHandle sessionId) {
    return getFormDataBodyPart("sessionid", sessionId);
  }
}
