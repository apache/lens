package org.apache.lens.cube.authorization;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.lens.server.api.authorization.ActionType;
import org.apache.lens.server.api.authorization.IAuthorizer;
import org.apache.lens.server.api.authorization.LensPrivilegeObject;

import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;

/**
 * Created by rajithar on 9/2/18.
 */
public class RangerLensAuthorizer implements IAuthorizer {

  public static RangerBasePlugin rangerBasePlugin;

  RangerLensAuthorizer(){
    this.init();
  }

  public void init() {
    if(rangerBasePlugin == null){
      synchronized(RangerLensAuthorizer.class) {
        rangerBasePlugin = new RangerBasePlugin("D1", "D1");
        rangerBasePlugin.setResultProcessor(new RangerDefaultAuditHandler());
        rangerBasePlugin.init();
      }
    }
  }

  @Override
  public boolean authorize(LensPrivilegeObject lensPrivilegeObject, ActionType accessType, Collection<String> userGroups) {

    RangerLensResource rangerLensResource = getLensResource(lensPrivilegeObject);

    RangerAccessRequest rangerAccessRequest = new RangerAccessRequestImpl(rangerLensResource, accessType.toString().toLowerCase() , null , userGroups == null ? null : new HashSet<String>(userGroups));

    RangerAccessResult rangerAccessResult = rangerBasePlugin.isAccessAllowed(rangerAccessRequest);

    return rangerAccessResult !=null && rangerAccessResult.getIsAllowed();
  }

  private RangerLensResource getLensResource(LensPrivilegeObject lensPrivilegeObject) {

    RangerLensResource lensResource = null;
    switch(lensPrivilegeObject.getObjectType()) {
    case COLUMN:
      lensResource = new RangerLensResource(LensObjectType.COLUMN, lensPrivilegeObject.getCubeOrFactOrDim(), lensPrivilegeObject.getColumn());
      break;

    case DIMENSION:
    case CUBE:
    case DIMENSIONTABLE:
    case STORAGE:
    case SEGMENTATION:
    case FACT:
      lensResource = new RangerLensResource(LensObjectType.TABLE, lensPrivilegeObject.getCubeOrFactOrDim(), null);
      break;

    case NONE:
    default:
      break;
    }
    return lensResource;
  }

  enum LensObjectType { NONE, TABLE, COLUMN };
//
//  public static void main(String[] args) {
//    IAuthorizer authorizer = new RangerLensAuthorizer();
//    authorizer.authorize(new LensPrivilegeObject(LensPrivilegeObject.LensPrivilegeObjectType.CUBE, "sample_cube"), ActionType.CREATE , null);
//  }
}
