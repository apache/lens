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

import java.lang.annotation.Annotation;

import org.apache.lens.server.api.annotations.MultiPurposeResource;

import org.glassfish.jersey.server.model.Parameter;
import org.glassfish.jersey.server.model.ResourceMethod;

import com.google.common.base.Optional;
import lombok.AllArgsConstructor;
import lombok.NonNull;

/**
 * Model object wrapping ResourceMethod. Adds utility methods.
 */
@AllArgsConstructor
public class LensResourceMethod {
  @NonNull
  private ResourceMethod resourceMethod;

  /**
   * if handler method is annotated with {@link org.apache.lens.server.api.annotations.MultiPurposeResource}, extract
   * that and return {@link org.apache.lens.server.api.annotations.MultiPurposeResource#formParamName()}
   *
   * @return
   */
  public Optional<String> getMultiPurposeFormParam() {
    return getHandlerAnnotation(MultiPurposeResource.class).isPresent()
      ? Optional.of(getHandlerAnnotation(MultiPurposeResource.class).get().formParamName()) : Optional.<String>absent();
  }

  /**
   * className.methodname.httpmethod
   *
   * @return qualified name of the handler method
   */
  public String name() {
    return new StringBuilder()
      .append(resourceMethod.getInvocable().getHandlingMethod().getDeclaringClass().getCanonicalName())
      .append(".")
      .append(resourceMethod.getInvocable().getHandlingMethod().getName())
      .append(".")
      .append(resourceMethod.getHttpMethod())
      .toString();
  }

  /**
   * Get annotation of class clz on the handler method
   *
   * @param <T>
   * @param clz annotation class
   * @return
   */
  private <T extends Annotation> Optional<T> getHandlerAnnotation(Class<T> clz) {
    return Optional.fromNullable(resourceMethod.getInvocable().getHandlingMethod().getAnnotation(clz));
  }

  /**
   * Extract default value of a parameter from the parameter annotations of the handler method
   *
   * @param paramName value of {@link org.glassfish.jersey.media.multipart.FormDataParam} annotation on the handler
   *                  method. Which will be the name of the argument in the request parameters
   * @return value of {@link javax.ws.rs.DefaultValue} annotation on the handler method
   */
  public String getDefaultValueForParam(final String paramName) {
    for (Parameter param : resourceMethod.getInvocable().getParameters()) {
      if (param.getSourceName().equals(paramName)) {
        return param.getDefaultValue();
      }
    }
    return null;
  }
}
