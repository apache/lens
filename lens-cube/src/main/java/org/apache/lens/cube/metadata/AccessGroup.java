package org.apache.lens.cube.metadata;

import static org.apache.lens.cube.metadata.CubeTableType.ACCESS_GROUP;

import java.util.*;

import org.apache.lens.server.api.authorization.ActionType;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;

import lombok.Getter;

public class AccessGroup extends AbstractCubeTable{

  private static final List<FieldSchema> COLUMNS = new ArrayList<FieldSchema>();

  @Getter
  private Set<User> userList;

  @Getter
  private ActionType actionType;
  static {
    COLUMNS.add(new FieldSchema("dummy", "string", "dummy column"));
  }
  public AccessGroup(String name, ActionType actionType, Map<String, String> properties, Set<User> userList) {
    super(name, COLUMNS, properties, 0L);
    this.actionType = actionType;
    this.userList = userList;
    addProperties();
  }

  private static void setUserProperties(Map<String, String> props, Set<User> users) {
    for (User user : users) {
      user.addProperties(props);
    }
  }

  protected AccessGroup(Table hiveTable) {
    super(hiveTable);
    this.actionType = getAccessGroupAction(getProperties());
    this.userList = getUserList(getName(), getProperties());
  }

  private Set<User> getUserList(String name, Map<String, String> properties) {
    Set<User> users = new HashSet<>();
    String userListString = MetastoreUtil.getNamedStringValue(properties, MetastoreUtil.getAccessGroupUserPrefix(name));
    for(String userName : userListString.split(",")) {
      User user = new User(userName, properties.get(MetastoreUtil.getAccessGroupUserEmailIdPrefix(userName)));
      users.add(user);
    }
    return users;
  }

  private ActionType getAccessGroupAction(Map<String, String> properties) {
    return ActionType.valueOf(properties.get(MetastoreConstants.ACCESS_GROUP_ACTION_KEY));
  }

  @Override
  public CubeTableType getTableType() {
    return ACCESS_GROUP;
  }

  @Override
  public Set<String> getStorages() {
    throw new NotImplementedException();
  }

  @Override
  protected void addProperties() {
    super.addProperties();

    getProperties().put(MetastoreUtil.getAccessGroupClassKey(getName()), getClass().getCanonicalName());
    MetastoreUtil.addNameStrings(getProperties(), MetastoreUtil.getAccessGroupUserPrefix(getName()), userList);
    setUserProperties(getProperties(), userList);
    addAccessAction(getName(), actionType, getProperties());
  }

  private void addAccessAction(String name, ActionType actionType, Map<String, String> properties) {
    properties.put(MetastoreConstants.ACCESS_GROUP_ACTION_KEY, actionType.getActionName());
  }
//
//  static AccessGroup createInstance(Table tbl) throws LensException {
//    String accessGroupName = tbl.getTableName();
//    String accessGroupClassName = tbl.getParameters().get(MetastoreUtil.getAccessGroupClassKey(accessGroupName));
//    try {
//      Class<?> clazz = Class.forName(accessGroupClassName);
//      Constructor<?> constructor = clazz.getConstructor(Table.class);
//      return (AccessGroup) constructor.newInstance(tbl);
//    } catch (Exception e) {
//      throw new LensException("Could not create access group class" + accessGroupClassName, e);
//    }
//  }

  public static class User implements Named {
    @Getter
    private String name;
    @Getter
    private String emailId;

    public User(String name, String emailId) {
      this.name = name;
      this.emailId = emailId;
    }

    public void addProperties(Map<String, String> props) {
      props.put(MetastoreUtil.getAccessGroupUserEmailIdPrefix(this.name), this.emailId);
    }
  }
}
