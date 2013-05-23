package com.inmobi.grill.es.cube.factory;

import java.util.Map;

import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

public class CubeReduceScriptFactory implements NativeScriptFactory {

  @Override
  public ExecutableScript newScript(Map<String, Object> params) {
    return null;
  }

}
