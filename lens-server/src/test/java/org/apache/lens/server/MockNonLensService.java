package org.apache.lens.server;

import org.apache.hive.service.AbstractService;

/**
 * The Class MockNonLensService.
 */
public class MockNonLensService extends AbstractService {

  /**
   * Instantiates a new mock non lens service.
   *
   * @param name
   *          the name
   */
  public MockNonLensService(String name) {
    super(name);
  }

  /**
   * Instantiates a new mock non lens service.
   */
  public MockNonLensService() {
    this(NAME);
  }

  /** The Constant NAME. */
  public static final String NAME = "MOCK_SVC";
}
