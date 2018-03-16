package org.apache.lens.cube.authorization;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;

/**
 * Created by rajithar on 8/2/18.
 */
public class RangerServiceTest extends RangerBaseService {

  @Override
  public void init(RangerServiceDef serviceDef, RangerService service){
    super.init(serviceDef, service);

  }

  @Override
  public Map<String, Object> validateConfig() throws Exception {
    return new HashedMap();
  }

  @Override
  public List<String> lookupResource(ResourceLookupContext resourceLookupContext) throws Exception {
    return null;
  }
}
