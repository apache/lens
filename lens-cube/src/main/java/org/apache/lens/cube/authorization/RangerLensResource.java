package org.apache.lens.cube.authorization;

import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

/**
 * Created by rajithar on 16/2/18.
 */

public class RangerLensResource extends RangerAccessResourceImpl {

  public static final String KEY_TABLE = "table";
  public static final String KEY_COLUMN = "column";

  RangerLensResource(RangerLensAuthorizer.LensObjectType objectType, String cubeOrDimOrFact, String column) {

    switch(objectType) {

    case COLUMN:
      setValue(KEY_TABLE, cubeOrDimOrFact);
      setValue(KEY_COLUMN, column);
      break;

    case TABLE:
      setValue(KEY_TABLE, cubeOrDimOrFact);
      break;

    case NONE:
    default:
      break;
    }
  }
}
