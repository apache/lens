package com.inmobi.grill.es.cube.script;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.NumericDocFieldData;
import org.elasticsearch.index.field.data.strings.StringDocFieldData;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.DocLookup;
import org.elasticsearch.search.lookup.FieldsLookup;

import com.inmobi.grill.es.cube.join.DimJoiner;
import com.inmobi.grill.es.cube.join.DimJoinerFactory;
import com.inmobi.grill.es.cube.join.JoinConfig;

/**
 * Mapper script that aggregates recrods matching a query, and joins with metadata tables
 */
@SuppressWarnings("rawtypes")
public class CubeMapScript extends AbstractSearchScript {
  public static final String NAME = "cube_map";
  public static final ESLogger LOG = Loggers.getLogger(CubeMapScript.class);
  
  /**
   * Hold aggregation result set and request parameters.
   */
  private Map<String, Object> session;
  // group records by values of these fields
  private List groupFields;
  
  // Sort order fields
  private List orderByFields;
  
  // Aggregate expressions
  private List aggregates;
  
  // Fields to load from main table
  private List mainTblFields;

  // Join chains leading main table fields to joined tables
  private Map joinChains;
  
  private boolean replaceMainField[];
  private int replaceFields[];

  private GroupResultSet resultSet;

  private List measures;
  private AggregateFunctionFactory funcFactory;
  private Map<String, DimJoiner> joiners;
  private Map<String, JoinConfig> joinerConfig;
  
  public CubeMapScript(Map<String, Object> params) {
    this.session = params;
    
    try {
      init();
    } catch (IOException e) {
      LOG.error("Unable to create map script", e);
    }
  }
  
  protected void init() throws IOException {
    groupFields = (List) session.get("groupby");
    aggregates = (List) session.get("aggregates");
    measures = (List) session.get("measures");
    
    funcFactory = setupFuncFactory(aggregates);
    
    if (session.containsKey("main_table_fields")) {
      /*
       * "main_table_fields": ["fact.c1", "fact.c2",  ... "fact.cN"]
       */
      mainTblFields = (List) session.get("main_table_fields");
      LOG.info("Got main table fields: {}", mainTblFields == null ? "_EMPTY__" : mainTblFields.toString());
    }
    
    if (session.containsKey("join_chains")) {
       /* Join chain config should be passed like this -
        *  "join_chains" :{
       *    "fact.col1" : { 
       *      "join_column": "id",
       *      "table": "adgroup",
       *      "type": "inner"/left_outer
       *      "get_columns": ["col1", ... "colN"]
       *      "condition": "equal"
       *     },
       *     
       *     "fact.col5" : {
       *         ....
       *         ....
       *      }
       *  }
       */
      
      joinChains = (Map) session.get("join_chains");
      if (LOG.isDebugEnabled()) {
        LOG.debug("GOT JOIN CHAINS: {}" , joinChains == null ? "_EMPTY_" : joinChains.toString());
      }
    }
    
    if (mainTblFields == null) {
      mainTblFields = groupFields;
    }
    
    // Join setup
    ArrayList<Integer> replaced = new ArrayList<Integer>();
    
    joiners = new HashMap<String, DimJoiner>();
    joinerConfig = new HashMap<String, JoinConfig>();
    replaceMainField = new boolean[mainTblFields.size()];
    
    for (int i = 0; i < mainTblFields.size(); i++) {
      String field = (String) mainTblFields.get(i);
      replaceMainField[i] = joinChains != null && joinChains.containsKey(field);
      
      if (replaceMainField[i]) {
        Map joinChainCfg = (Map) joinChains.get(field);
        String table = (String) joinChainCfg.get("table");
        String column = (String) joinChainCfg.get("join_column");
        String condition = (String) joinChainCfg.get("condition");
        String joinType = (String) joinChainCfg.get("type");
        List getColumns = (List) joinChainCfg.get("get_columns");
        JoinConfig cfg = new JoinConfig(table, column, field, joinType, condition, getColumns);
        
        joinerConfig.put(field, cfg);
        DimJoiner joiner = DimJoinerFactory.getJoinerForConfig(cfg);
        if (joiner == null) {
          LOG.warn("Joiner not found for {} to  {}.{}", field, table, column);
        } else {
          joiners.put(field, joiner);
        }
        replaced.add(Integer.valueOf(i));
      }
    }
    
    replaceFields = new int[replaced.size()];
    for (int i = 0; i < replaced.size(); i++) {
      replaceFields[i] = replaced.get(i);
    }
    
    LOG.info("Fields to be replaced: {}", Arrays.toString(replaceFields));
    
    // Sort order setup
    if (session.containsKey("orderBy")) {
      /*
       * "orderBy": ["f1", "f2" ... "fn"]
       */
      orderByFields = (List) session.get("orderBy");
    }
    
    
    if (!session.containsKey(GroupResultSet.RESULT_SET_KEY)) {
      Comparator<GroupEntry> sorter = buildComparator(orderByFields);
      resultSet = new GroupResultSet(sorter);
      session.put(GroupResultSet.RESULT_SET_KEY, resultSet);
    }
  }
   
  abstract class SortField {
    final int fieldPosition;
    public SortField(int pos) {
      this.fieldPosition = pos;
    }
    public abstract Object getSortValue(GroupEntry entry);
  }
  
  /**
   * Make a comparator to sort result set based on user defined columns.
   * @param orderByFields list of columns given in the order by part of the query
   * @return comparator that can be used to sort result set.
   */
  private Comparator<GroupEntry> buildComparator(List orderByFields) {
    if (orderByFields == null || orderByFields.isEmpty()) {
      // User skipped order by clause, so sort by natural order of grouped fields.
      return new Comparator<GroupEntry>() {
        @Override
        public int compare(GroupEntry g1, GroupEntry g2) {
          // Assume both g1 and g2 have the same fields.
          for (int i = 0; i < g1.group.fields.length; i++) {
            String f1 = g1.group.fields[i];
            String f2 = g2.group.fields[i];
            if (f1 != null && f2 != null) {
              int cmp = f1.compareTo(f2);
              if (cmp != 0) {
                return cmp;
              }
            } else {
              if (f1 == null && f2 != null) {
                return 1;
              } else if (f1 != null && f2 == null) {
                return -1;
              }
            }
          }
          return 0;
        }
      };
    } else {
      // Need to construct sorter based on group fields as well as aggregate values.
      final SortField[] sortFields = new SortField[orderByFields.size()];
      
      for (int i = 0; i < orderByFields.size(); i++) {
        String sortField = (String) orderByFields.get(i);
        int sortFieldPos = groupFields.indexOf(sortField);
        if (sortFieldPos != -1) {
          // this order by field is a group field, so we can return the string 
          // value of the group field.
          sortFields[i] = new SortField(sortFieldPos) {
            @Override
            public String getSortValue(GroupEntry entry) {
              return entry.group.fields[fieldPosition];
            }
          };
        } else {
          // maybe this field is a name of the function
          boolean fieldFound = false;
          for (int j = 0; j < aggregates.size(); i++) {
            Map aggrFunctionConfig = (Map) aggregates.get(j);
            String agrgName = (String) aggrFunctionConfig.get("name");
            if (sortField.equals(agrgName)) {
              sortFields[i] = new SortField(j) {
                @Override
                public Object getSortValue(GroupEntry entry) {
                  return Double.valueOf(entry.functions[fieldPosition].get());
                }
              };
              
              fieldFound = true;
              break;
            }
          }
          
          if (!fieldFound) {
            // neither group field, nor aggregate function.
            // sorting can be done on items present in the result set only.
            throw new RuntimeException("Invalid order by field:" + sortField);
          }
        }
      }
      
      // Return sorter based on the user defined order by fields.
      return new Comparator<GroupEntry> () {
        @Override
        public int compare(GroupEntry g1, GroupEntry g2) {
          for (int i = 0; i < sortFields.length; i++) {
            SortField sf = sortFields[i];
            Comparable c1 = (Comparable) sf.getSortValue(g1);
            Comparable c2 = (Comparable) sf.getSortValue(g2);
            @SuppressWarnings("unchecked")
            int cmp = c1.compareTo(c2);
            if (cmp != 0) {
              return cmp;
            }
          }
          return 0;
        }
      };
    }
  }

  private AggregateFunctionFactory setupFuncFactory(List aggregates) {
    AggregateFunctionFactory factory = new AggregateFunctionFactory(measures);
    for (Object aggr : aggregates) {
      factory.addFunctionConfig((Map)aggr);
    }
    return factory;
  }


  /**
   * Called for every record in the main table by the facet. other runAs* methods can be ignored.
   */
  @Override
  public Object run() {
    DocLookup doc = super.doc();
    
    // Resolve joins to load dimension fields - dim foreign keys, dim fields needed in group by after running join condition
    // Populate main table row
    String[] group = new String[mainTblFields.size()];
    
    boolean shouldGroup = true;
    
    @SuppressWarnings("unchecked")
    List<Object> replaceValues[] = new List[replaceFields.length];
    int sizes[] = new int[replaceFields.length];
    int r = 0;
    
    // Populate the group with 'main' table values.
    for (int i = 0; i < group.length; i++) {
      String fieldName = (String) mainTblFields.get(i);
      Object data = doc.get(fieldName);
      if (! (data instanceof StringDocFieldData)) {
        LOG.warn("NOT FIELD DATA: {}" , fieldName);
        continue;
      }
      
      // Assume not analyzed strings
      String field = ((StringDocFieldData) data).getValue();
      
      if (!replaceMainField[i]) {
        group[i] = field;
      } else {
        List<Object> joinedValues = new ArrayList<Object>();
        String factval = field;
        shouldGroup = resolveJoinChain(factval, i/*factpos*/, joinedValues);
        replaceValues[r] = joinedValues;
        sizes[r++] = joinedValues.size();
        // Process this row only if join condition passed
        if (!shouldGroup) {
          break;
        }
      }
    }
    
    if (shouldGroup) {
      if (replaceFields.length == 0) {
        // nothing to join with
        handleGroup(group);
      } else {
        // number of groups generated = size of cartesian product of joined values for each join column
        int counters[] = new int[replaceFields.length];
  
        int permutations = 1;
        for (int size : sizes) {
          permutations *= (size == 0 ? 1 : size);
        }
        
        for (int p = 0; p < permutations; p++) {
          for (int i = 0; i < counters.length; i++) {
            int c = counters[i];
            // index of field to replace in the main row.
            int rpos = replaceFields[i];
            List<Object> rlist = replaceValues[i];
            if (rlist == null || rlist.isEmpty()) {
              group[rpos] = null;
            } else {
              group[rpos] = (String) rlist.get(c);
            }
          }
          
          // handle group here
          handleGroup(group);
          
          // Roll counters
          boolean stoproll = false;
          for (int j = counters.length - 1; j >= 0 && !stoproll; j--) {
            if (counters[j] < sizes[j] -1) {
              counters[j]++;
              stoproll = true;
            } else {
              counters[j] = 0;
            }
          }
        }
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping {}", Arrays.toString(group));
      }
    }
    
    // Return value is not processed by facet so just return null
    return null;
  }

  /**
   * Compute all possible replacements for a given joined column in main table given a current 
   * value of the column.
   * @param factval - Value of the field to replace in fact (main table)
   * @param fieldToReplace - position of the field to be replaced in the group
   * @param joinedValues - Values resulting after join
   * @return true if join was successful, false if join condition was not met.
   */
  private boolean resolveJoinChain(String factval, int mainTblField, List<Object> joinedValues) {
    // Get joiner for this column in the fact.
    String factField = (String) mainTblFields.get(mainTblField);
    DimJoiner joiner = joiners.get(factField);
    if (joiner == null) {
      throw new RuntimeException("Null joiner " + factField);
    }
    String columnToGet = joinerConfig.get(factField).getColumnsToGet().get(0);
    return joiner.join(columnToGet, factval, joinedValues);
  }

  private void handleGroup(String[] fields) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Grouping {}", Arrays.toString(fields));
    }
    FieldGroup g = new FieldGroup(fields);
    GroupEntry entry = new GroupEntry();
    entry.group = g;
    entry.functions = computeAggregates();
    resultSet.put(entry);
  }
  
  /**
   * Compute aggregate functions.
   * @return
   */
  private AggregateFunction[] computeAggregates() {
    String measureVals[] = new String[measures.size()];
    for (int i = 0; i < measureVals.length; i++) {
      Object data = doc().get(measures.get(i));
      if ((data instanceof StringDocFieldData)) {
        measureVals[i] = ((StringDocFieldData)data).getValue();
      } else if (data instanceof NumericDocFieldData) {
        measureVals[i] = Double.toString( ((NumericDocFieldData)data).getDoubleValue());
      } else {
        LOG.warn("##### Unknown doc field type: {}", data.getClass().getName());
      }
    }
    List<AggregateFunction> flist = funcFactory.create(measureVals);
    
    return flist.toArray(new AggregateFunction[]{});
  }
}

