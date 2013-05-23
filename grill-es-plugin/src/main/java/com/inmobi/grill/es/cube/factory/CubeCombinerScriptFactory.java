package com.inmobi.grill.es.cube.factory;

import java.util.Map;

import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

import com.inmobi.grill.es.cube.script.CubeCombinerScript;

public class CubeCombinerScriptFactory implements NativeScriptFactory {

  @Override
  public ExecutableScript newScript(Map<String, Object> params) {
    return new CubeCombinerScript(params);
  }

}
