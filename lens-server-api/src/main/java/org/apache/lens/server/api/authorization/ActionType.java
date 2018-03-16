package org.apache.lens.server.api.authorization;

import lombok.Getter;

public enum ActionType {
  CREATE("create"),
  READ("read"),
  UPDATE("update"),
  DELETE("delete"),
  SELECT("select");

  @Getter
  private String actionName;

  ActionType(String actionName){
    this.actionName = actionName;
  }
}
