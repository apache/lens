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
package org.apache.lens.server.session;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;

import org.apache.lens.api.LensSessionHandle;

@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = ValidSession.Validator.class)
public @interface ValidSession {
  String message() default "{org.apache.lens.server.session.ValidSession.message}";

  Class<?>[] groups() default {};

  Class<? extends Payload>[] payload() default {};

  class Validator implements ConstraintValidator<ValidSession, LensSessionHandle> {

    @Override
    public void initialize(ValidSession validSession) {

    }

    @Override
    public boolean isValid(LensSessionHandle lensSessionHandle, ConstraintValidatorContext constraintValidatorContext) {
      return true;
    }
  }
}
