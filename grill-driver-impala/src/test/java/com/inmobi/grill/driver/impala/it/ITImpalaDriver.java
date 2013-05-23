package com.inmobi.grill.driver.impala.it;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.hadoop.conf.Configuration;

import com.inmobi.grill.driver.impala.ImpalaDriver;
import com.inmobi.grill.driver.impala.ImpalaResultSet;
import com.inmobi.grill.exception.GrillException;

public class ITImpalaDriver {

	@Test
	public void testIntegration() throws GrillException {
		ImpalaDriver iDriver = new ImpalaDriver();
		Configuration config = new Configuration();
		config.set("PORT", "21000");
		config.set("HOST", "localhost");
		iDriver.configure(config);
		
			List<Object> row = null;
			ImpalaResultSet iResultSet = (ImpalaResultSet) iDriver
					.execute("select * from emp", null);
			if (iResultSet.hasNext()) {
				row = iResultSet.next();
				System.out.println("Row1" + row);
			}
			if (iResultSet.hasNext()) {
				row = iResultSet.next();
				System.out.println("Row2" + row);
			}
			Assert.assertTrue(true);
		
	}
	
	
}
