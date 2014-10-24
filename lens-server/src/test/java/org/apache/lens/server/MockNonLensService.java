package org.apache.lens.server;

import org.apache.hive.service.AbstractService;

public class MockNonLensService extends AbstractService {
  public MockNonLensService(String name) {
    super(name);
  }

  public MockNonLensService() {
    this(NAME);
  }

  public static final String NAME = "MOCK_SVC";
}