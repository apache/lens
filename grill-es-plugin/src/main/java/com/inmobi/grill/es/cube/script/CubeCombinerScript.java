package com.inmobi.grill.es.cube.script;

import java.util.Map;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.script.AbstractExecutableScript;

/**
 * Convert result set to serializable form.
 * Optional: Apply HAVING and sorting here.
 */
public class CubeCombinerScript extends AbstractExecutableScript {
  public static final ESLogger LOG = Loggers.getLogger(CubeCombinerScript.class);
  private Map<String, Object> session;
  
  public CubeCombinerScript(Map<String, Object> params) {
    this.session = params;
  }

  public static final String NAME = "cube_combiner";

  @Override
  public Object run() {
    GroupResultSet resultSet = (GroupResultSet) session.remove(GroupResultSet.RESULT_SET_KEY);
    if (resultSet == null) {
      throw new RuntimeException("Result set was not found");
    }
    
    LOG.info("Combiner processing result set of size:{} ", resultSet.size());
    resultSet.serializeToList(session);
    return session.get(GroupResultSet.SERIALIZED_RESULT_SET_KEY);
  }
  
  
}
