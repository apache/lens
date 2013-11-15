package com.inmobi.grill.metastore.service;

import javax.ws.rs.core.MediaType;


public class TestMetastoreServiceJSON extends TestMetastoreService {
  public TestMetastoreServiceJSON() {
    mediaType = MediaType.APPLICATION_JSON;
  }
}
