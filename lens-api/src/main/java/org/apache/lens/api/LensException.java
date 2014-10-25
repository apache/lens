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
/*
 * 
 */
package org.apache.lens.api;

/**
 * The Class LensException.
 */
@SuppressWarnings("serial")
public class LensException extends Exception {

  /**
   * Instantiates a new lens exception.
   *
   * @param msg
   *          the msg
   */
  public LensException(String msg) {
    super(msg);
  }

  /**
   * Instantiates a new lens exception.
   *
   * @param msg
   *          the msg
   * @param th
   *          the th
   */
  public LensException(String msg, Throwable th) {
    super(msg, th);
  }

  /**
   * Instantiates a new lens exception.
   */
  public LensException() {
    super();
  }

  /**
   * Instantiates a new lens exception.
   *
   * @param th
   *          the th
   */
  public LensException(Throwable th) {
    super(th);
  }
}
