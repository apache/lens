/*
 * 
 */
package org.apache.lens.api.query;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * The Enum ResultColumnType.
 */
@XmlRootElement
public enum ResultColumnType {

  /** The boolean. */
  BOOLEAN, // boolean
  /** The tinyint. */
  TINYINT, // short
  /** The smallint. */
  SMALLINT, // short
  /** The int. */
  INT, // int
  /** The bigint. */
  BIGINT, // long
  /** The float. */
  FLOAT, // float
  /** The double. */
  DOUBLE, // double
  /** The string. */
  STRING, // string
  /** The timestamp. */
  TIMESTAMP, // Date
  /** The binary. */
  BINARY, // byte[]
  /** The array. */
  ARRAY, // List
  /** The map. */
  MAP, // Map
  /** The struct. */
  STRUCT,

  /** The uniontype. */
  UNIONTYPE,

  /** The user defined. */
  USER_DEFINED,

  /** The decimal. */
  DECIMAL,

  /** The null. */
  NULL,

  /** The date. */
  DATE,

  /** The varchar. */
  VARCHAR,

  /** The char. */
  CHAR;
}
