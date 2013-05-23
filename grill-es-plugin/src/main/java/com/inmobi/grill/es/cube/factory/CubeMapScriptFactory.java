package com.inmobi.grill.es.cube.factory;

import java.util.Map;

import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

import com.inmobi.grill.es.cube.script.CubeMapScript;

public class CubeMapScriptFactory  implements NativeScriptFactory {
  @Override
  public ExecutableScript newScript(Map<String, Object> params) {
    return new CubeMapScript(params);
  }

}
