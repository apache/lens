package org.apache.lens.server;

import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.ui.UIApp;
import org.glassfish.jersey.filter.LoggingFilter;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.Set;

import static org.testng.Assert.assertEquals;

/**
 * The Class TestStaticFileResource.
 */
@Test(groups = "unit-test")
public class TestStaticFileResource extends LensJerseyTest {

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

  @Override
  protected int getTestPort() {
    return 19999;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.glassfish.jersey.test.JerseyTest#configure()
   */
  @Override
  protected Application configure() {
    return new UIApp() {
      @Override
      public Set<Class<?>> getClasses() {
        Set<Class<?>> classes = super.getClasses();
        classes.add(LoggingFilter.class);
        return classes;
      }
    };
  }

  @Override
  protected URI getUri() {
    return UriBuilder.fromUri("http://localhost/").port(getTestPort()).build();
  }

  @Override
  protected URI getBaseUri() {
    return getUri();
  }

  /**
   * Test static file resource.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testStaticFileResource() throws Exception {
    LensServices.get().getHiveConf().set(LensConfConstants.SERVER_UI_STATIC_DIR, "src/main/webapp/static");
    LensServices.get().getHiveConf().setBoolean(LensConfConstants.SERVER_UI_ENABLE_CACHING, false);

    System.out.println("@@@@ " + target().path("index.html").getUri());
    Response response = target().path("index.html").request().get();
    assertEquals(response.getStatus(), 200);

    response = target().path("index234.html").request().get();
    assertEquals(response.getStatus(), 404);
  }

}
