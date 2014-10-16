package org.apache.lens.ml;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TrainerArgParser {
  public abstract static class CustomArgParser<E> {
    public abstract E parse(String value);
  }

  public static final Log LOG = LogFactory.getLog(TrainerArgParser.class);

  /**
   * Extracts feature names. If the trainer has any parameters associated with @TrainerParam
   * annotation, those are set as well.
   * @param trainer
   * @param args
   * @return List of feature column names.
   */
  public static List<String> parseArgs(MLTrainer trainer, String[] args) {
    List<String> featureColumns = new ArrayList<String>();
    Class<? extends MLTrainer> trainerClass = trainer.getClass();
    // Get param fields
    Map<String, Field> fieldMap = new HashMap<String, Field>();

    for (Field fld : trainerClass.getDeclaredFields()) {
      fld.setAccessible(true);
      TrainerParam paramAnnotation = fld.getAnnotation(TrainerParam.class);
      if (paramAnnotation != null) {
        fieldMap.put(paramAnnotation.name(), fld);
      }
    }

    for (int i = 0; i < args.length; i += 2) {
      String key = args[i].trim();
      String value = args[i+1].trim();

      try {
        if ("feature".equalsIgnoreCase(key)) {
          featureColumns.add(value);
        } else if (fieldMap.containsKey(key)) {
          Field f = fieldMap.get(key);
          if (String.class.equals(f.getType())) {
            f.set(trainer, value);
          } else if (Integer.TYPE.equals(f.getType())) {
            f.setInt(trainer, Integer.parseInt(value));
          } else if (Double.TYPE.equals(f.getType())) {
            f.setDouble(trainer, Double.parseDouble(value));
          } else if (Long.TYPE.equals(f.getType())) {
            f.setLong(trainer, Long.parseLong(value));
          } else {
            // check if the trainer provides a deserializer for this param
            String customParserClass = trainer.getConf()
              .getProperties().get("lens.ml.args."+ key);
            if (customParserClass != null) {
              Class<? extends CustomArgParser<?>> clz =
                (Class<? extends CustomArgParser<?>>) Class.forName(customParserClass);
              CustomArgParser<?> parser = clz.newInstance();
              f.set(trainer, parser.parse(value));
            } else {
              LOG.warn("Ignored param " + key + "=" + value + " as no parser found");
            }
          }
        }
      } catch (Exception exc) {
        LOG.error("Error while setting param " + key
          + " to " + value + " for trainer " + trainer);
      }
    }
    return featureColumns;
  }
}
