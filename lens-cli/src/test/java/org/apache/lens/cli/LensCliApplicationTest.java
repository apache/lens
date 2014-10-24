package org.apache.lens.cli;

import org.apache.lens.server.LensAllApplicationJerseyTest;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;

/**
 * The Class LensCliApplicationTest.
 */
public class LensCliApplicationTest extends LensAllApplicationJerseyTest {

  @Override
  protected int getTestPort() {
    return 9999;
  }

  @Override
  protected URI getBaseUri() {
    return UriBuilder.fromUri(getUri()).path("lensapi").build();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#setUp()
   */
  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#tearDown()
   */
  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
  }

}
