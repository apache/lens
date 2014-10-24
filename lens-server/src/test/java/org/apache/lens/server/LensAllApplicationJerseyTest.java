package org.apache.lens.server;

import javax.ws.rs.core.Application;

import org.apache.lens.server.LensApplication;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

/**
 * The Class LensAllApplicationJerseyTest.
 */
public abstract class LensAllApplicationJerseyTest extends LensJerseyTest {

  /*
   * (non-Javadoc)
   * 
   * @see org.glassfish.jersey.test.JerseyTest#configure()
   */
  @Override
  protected Application configure() {
    return new LensApplication();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.glassfish.jersey.test.JerseyTest#configureClient(org.glassfish.jersey.client.ClientConfig)
   */
  @Override
  protected void configureClient(ClientConfig config) {
    config.register(MultiPartFeature.class);
  }

}
