package com.inmobi.grill.api.query;

/*
 * #%L
 * Grill API
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

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public enum ResultColumnType {
  BOOLEAN, // boolean
  TINYINT, // short
  SMALLINT,// short
  INT, // int
  BIGINT, // long
  FLOAT, // float
  DOUBLE, // double
  STRING, // string
  TIMESTAMP, // Date
  BINARY, // byte[]
  ARRAY, // List
  MAP, // Map
  STRUCT, 
  UNIONTYPE,
  USER_DEFINED,
  DECIMAL,
  NULL,
  DATE,
  VARCHAR,
  CHAR;
}
