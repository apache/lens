package com.inmobi.grill.es.cube.script;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AggregateFunctionFactory {
  
  private static Map<String, Class<? extends AggregateFunction>> clsMap;
  
  static {
    // Register aggregate functions here.
    clsMap = new HashMap<String, Class<? extends AggregateFunction>>();
    clsMap.put("sum", SumDouble.class);
    clsMap.put("max", MaxDouble.class);
    clsMap.put("count", CountDouble.class);
    clsMap.put("avg", AvgDouble.class);
    clsMap = Collections.unmodifiableMap(clsMap);
  }
  
  public static class FunctionConfig {
    final String name;
    final List<String> argFields;
    final Class<? extends AggregateFunction> funClass;
    
    public FunctionConfig (String name, List<String> argFields) {
      this.name = name;
      this.argFields = argFields;
      funClass = clsMap.get(name);
    }
    
    public AggregateFunction newFunction(String[] values) {
      try {
        AggregateFunction fn = funClass.newInstance();
        fn.update(values);
        return fn;
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
      return null;
    }
  }
  
  final List<FunctionConfig> queryFunctions;
  final List<String> measures;
  // tells which measure is applicable to which all function configs
  final List<BitSet> argPosMaps;
  
  public AggregateFunctionFactory(List<String> measures) {
    this.measures = measures;
    this.queryFunctions = new ArrayList<FunctionConfig>();
    argPosMaps = new ArrayList<BitSet>();
  }
  
  /**
   * Setup the factory with config passed as query parameters.
   * Later, during record processing, the factory is used to create aggregates function objects
   * @param cfgmap
   */
  @SuppressWarnings("rawtypes")
  public void addFunctionConfig(Map cfgmap) {
    String name = (String) cfgmap.get("name");
    List argumentFields = (List) cfgmap.get("arguments");
    
    queryFunctions.add(new FunctionConfig(name, argumentFields));

    BitSet cfgset = new BitSet();
    for (int i = 0; i < argumentFields.size(); i++) {
      String field = (String) argumentFields.get(i);
      // get the measure
      int mpos = measures.indexOf(field);
      cfgset.set(mpos);
    }
    
    argPosMaps.add(cfgset);
  }
  
  /**
   * Create functions initiated with values passed as arguments.
   * @param values
   * @return
   */
  public List<AggregateFunction> create(String[] values) {
    List<AggregateFunction> functions = new ArrayList<AggregateFunction>(queryFunctions.size());;
    // Decide which value to pass to which function
    for (int i = 0; i < queryFunctions.size(); i++) {
      FunctionConfig cfg = queryFunctions.get(i);
      
      int k = -1;
      int j = 0;
      BitSet cfgset = argPosMaps.get(i);
      String args[] = new String[cfgset.cardinality()];
      
      while ((k = cfgset.nextSetBit(k + 1)) >= 0) {
        args[j++] = values[k];
      }
      
      functions.add(cfg.newFunction(args));
     }
    return functions;
  }
  
  @SuppressWarnings("rawtypes")
  public static AggregateFunction create(String name, Object value) {
    Class<? extends AggregateFunction> cls = clsMap.get(name);
    if (cls != null) {
      try {
        AggregateFunction fn = cls.newInstance();
        fn.set((Double) value);
        return fn;
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
    }
    
    return null;
  }
}
