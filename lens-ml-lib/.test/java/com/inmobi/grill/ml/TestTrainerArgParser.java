package com.inmobi.grill.ml;

import com.inmobi.grill.ml.spark.trainers.KMeansTrainer;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestTrainerArgParser {
  @Test
  public void testTrainerArgParser() throws Exception {
    KMeansTrainer trainer = new KMeansTrainer();

    String args[] = {
      "feature", "f1",
      "feature", "f2",
      "feature", "f3",
      "partition", "foo",
      "k", "100",
      "maxIterations", "200",
      "runs", "300",
      "initializationMode", "random"
    };

    List<String> features = TrainerArgParser.parseArgs(trainer, args);
    assertNotNull(features);
    assertEquals(3, features.size());
    assertEquals(features, Arrays.asList("f1", "f2", "f3"));

    assertField(trainer, "partFilter", "foo");
    assertField(trainer, "k", 100);
    assertField(trainer, "maxIterations", 200);
    assertField(trainer, "runs", 300);
    assertField(trainer, "initializationMode", "random");

  }

  public void assertField(MLTrainer trainer, String field, Object value)
  throws Exception {
    Field fld = trainer.getClass().getDeclaredField(field);
    fld.setAccessible(true);
    assertEquals(fld.get(trainer), value, "Field= " + field
     + " Expected=" + value + " Actual=" + fld.get(trainer));
  }
}
