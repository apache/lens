package com.inmobi.grill.es.cube.script;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.script.AbstractExecutableScript;

public class CubeReducerScript extends AbstractExecutableScript {
  public static final String NAME = "cube_reduce";
  
  private Map<String, Object> session;
  
  public CubeReducerScript(Map<String, Object> params) {
    this.session = params;
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public Object run() {
    List resultSetLists = (List) session.get("facets");
    Iterator resultSetItr = resultSetLists.iterator();
    
    if (!resultSetItr.hasNext()) {
      return null;
    }
    
    List<List<?>> rows = (List<List<?>>) resultSetItr.next();
    GroupResultSet first = new GroupResultSet();
    first.readFromSession(rows);
    resultSetItr.remove();
    
    while (resultSetItr.hasNext()) {
      GroupResultSet set = new GroupResultSet();
      set.readFromSession((List<List<?>>) resultSetItr.next());
      Iterator<GroupEntry> entryItr = set.entryIterator();
      while (entryItr.hasNext()) {
        first.put(entryItr.next());
      }
      resultSetItr.remove();
    }
    
    first.serializeToList(session);
    return session.remove(GroupResultSet.SERIALIZED_RESULT_SET_KEY);
  }

}
