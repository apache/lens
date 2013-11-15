package com.inmobi.grill.client.api;

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
  VARCHAR;
}
