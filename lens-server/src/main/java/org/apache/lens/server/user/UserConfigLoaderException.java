package org.apache.lens.server.user;


import java.sql.SQLException;

import org.apache.lens.api.LensException;

/*
 * #%L
 * Grill Server
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

public class UserConfigLoaderException extends RuntimeException {

  public UserConfigLoaderException() {
    super();
  }

  public UserConfigLoaderException(String s) {
    super(s);
  }

  public UserConfigLoaderException(Throwable e) {
    super(e);
  }

  public UserConfigLoaderException(String message, Throwable cause) {
    super(message, cause);
  }
}
