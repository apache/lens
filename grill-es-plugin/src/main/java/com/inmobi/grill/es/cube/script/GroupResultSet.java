package com.inmobi.grill.es.cube.script;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class GroupResultSet {
  public static final ESLogger LOG = Loggers.getLogger(GroupResultSet.class);
  public static final String RESULT_SET_KEY = "result_set";
  public static final String SERIALIZED_RESULT_SET_KEY = "serialized_result_set";
  private HashMap<FieldGroup, GroupEntry> resultset;
  private TreeSet<GroupEntry> orderedEntries;

  public GroupResultSet() {
    
  }
  
  public GroupResultSet(Comparator<GroupEntry> sorter) {
    resultset = new HashMap<FieldGroup, GroupEntry>();
    orderedEntries = new TreeSet<GroupEntry>(sorter);
  }

  public GroupEntry getGroup(FieldGroup g) {
    return resultset.get(g);
  }

  @SuppressWarnings("unchecked")
  public void put(GroupEntry entry) {
    GroupEntry prev = resultset.get(entry.group);
    if (prev == null) {
      
      prev = entry;
      resultset.put(entry.group, entry);
      if (orderedEntries != null) {
        orderedEntries.add(entry);
      }
    } else {
      for (int i = 0; i < prev.functions.length; i++) {
        prev.functions[i].update(entry.functions[i].get());
      }
    }
  }

  public Iterator<GroupEntry> entryIterator() {
    return resultset.values().iterator();
  }

  public Iterator<GroupEntry> sortedIterator() {
    return orderedEntries.iterator();
  }

  public int size() {
    return resultset.size();
  }

  public void serializeToList(Map<String, Object> session) {
    // Serializable map
    List<List<?>> rows = new ArrayList<List<?>>();
    Iterator<GroupEntry> entryItr = entryIterator();

    if (entryItr.hasNext()) {
      GroupEntry first = entryItr.next();
      
      // Put function names
      List<String> fnames = new ArrayList<String>(first.functions.length);
      for (AggregateFunction fn : first.functions) {
        fnames.add(fn.name());
      }
      // First row is names of functions
      rows.add(fnames);

      // Add fields, followed by function values
      final int nFields = first.group.fields.length;
      final int nFunctions = first.functions.length;

      addRowEntry(first, nFields, nFunctions, rows);
      while (entryItr.hasNext()) {
        addRowEntry(entryItr.next(), nFields, nFunctions, rows);
      }
    }
    session.put(SERIALIZED_RESULT_SET_KEY, rows);
  }

  public void addRowEntry(GroupEntry entry, int nFields, int nFunctions, List<List<?>> rows) {
    
    List<String> fields = new ArrayList<String>();
    for (int i = 0; i < nFields; i++) {
      fields.add(entry.group.fields[i]);
    }

    List<Object> funcValues = new ArrayList<Object>();
    for (int i = 0; i < nFunctions; i++) {
      funcValues.add(entry.functions[i].get());
    }

    rows.add(fields);
    rows.add(funcValues);
    
  }
  
  
  public void readFromSession(List<List<?>> rows) {
    if (rows == null) {
      return ;
    }
    
    Iterator<List<?>> rowItr = rows.iterator();

    this.resultset = new HashMap<FieldGroup, GroupEntry>();
    List firstList = rowItr.next();
    String fnames[] = new String[firstList.size()];
    for (int i = 0; i < firstList.size(); i++){
      fnames[i] = (String) firstList.get(i);
    }
    
    //Read result set data
    boolean even = true;
    FieldGroup fgroup = null;
    
    while (rowItr.hasNext()) {
      List row = rowItr.next();
      if (even) {
        // Field values
        
        String fieldValues[] = new String[row.size()];
        for (int i = 0; i< fieldValues.length; i++) {
          fieldValues[i] = (String) row.get(i);
        }
        fgroup = new FieldGroup(fieldValues);
      } else {
        // Function values
        AggregateFunction functionValues[] = new AggregateFunction[row.size()];
        for (int i = 0; i < functionValues.length; i++) {
          functionValues[i] = AggregateFunctionFactory.create(fnames[i], row.get(i));
        }
        GroupEntry entry = new GroupEntry();
        entry.group = fgroup;
        entry.functions = functionValues;
        put(entry);
      }
      // toggle even odd
      even = !even;
    }
  }
}
