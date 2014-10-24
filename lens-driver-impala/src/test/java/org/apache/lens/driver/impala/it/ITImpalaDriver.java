package org.apache.lens.driver.impala.it;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.lens.api.LensException;
import org.apache.lens.driver.impala.ImpalaDriver;
import org.apache.lens.driver.impala.ImpalaResultSet;

/**
 * The Class ITImpalaDriver.
 */
public class ITImpalaDriver {

  /**
   * Test integration.
   *
   * @throws LensException
   *           the lens exception
   */
  @Test
  public void testIntegration() throws LensException {
    ImpalaDriver iDriver = new ImpalaDriver();
    Configuration config = new Configuration();
    config.set("PORT", "21000");
    config.set("HOST", "localhost");
    iDriver.configure(config);

    List<Object> row = null;
    ImpalaResultSet iResultSet = (ImpalaResultSet) iDriver.execute("select * from emp", null);
    if (iResultSet.hasNext()) {
      row = iResultSet.next().getValues();
      System.out.println("Row1" + row);
    }
    if (iResultSet.hasNext()) {
      row = iResultSet.next().getValues();
      System.out.println("Row2" + row);
    }
    Assert.assertTrue(true);

  }

}
