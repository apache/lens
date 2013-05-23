package com.inmobi.grill.es.plugin;

import java.io.IOException;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.script.ScriptModule;

import com.inmobi.grill.es.cube.factory.CubeCombinerScriptFactory;
import com.inmobi.grill.es.cube.factory.CubeMapScriptFactory;
import com.inmobi.grill.es.cube.factory.CubeReduceScriptFactory;
import com.inmobi.grill.es.cube.join.DimJoinerFactory;
import com.inmobi.grill.es.cube.script.CubeCombinerScript;
import com.inmobi.grill.es.cube.script.CubeMapScript;
import com.inmobi.grill.es.cube.script.CubeReducerScript;

/**
 * Registers cube scripts with elastic search.
 */
public class CubePlugin extends AbstractPlugin {
  public static final ESLogger LOG = Loggers.getLogger(CubePlugin.class);
  @Override
  public String description() {
    return "InMobi Cube Plugin for ElasticSearch";
  }

  @Override
  public String name() {
    return "cube";
  }
  
  public void onModule(ScriptModule scriptModule) {
    // Each script is registered by giving its string name, and a factory class
    scriptModule.registerScript(CubeMapScript.NAME, CubeMapScriptFactory.class);
    scriptModule.registerScript(CubeCombinerScript.NAME, CubeCombinerScriptFactory.class);
    scriptModule.registerScript(CubeReducerScript.NAME, CubeReduceScriptFactory.class);
    try {
      DimJoinerFactory.setup();
      LOG.info("Setup joiner factory");
    } catch (IOException e) {
      LOG.error("Error setting up joiner factory", e);
    }
    LOG.info("Started cube plugin");
  }
}
