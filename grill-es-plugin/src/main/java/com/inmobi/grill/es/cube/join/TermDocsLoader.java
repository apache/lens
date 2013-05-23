package com.inmobi.grill.es.cube.join;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.util.OpenBitSet;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.CacheLoader;
import org.elasticsearch.common.cache.LoadingCache;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

/**
 * This loader reads values from a Lucene index reader.
 * Loader is created per table so a single loader can serve across many joined columns of the same table.
 */
public class TermDocsLoader extends DimLoader {
  public static final ESLogger LOG = Loggers.getLogger(TermDocsLoader.class);
  public static final int FIELD_CACHE_EXPIRY_DURATION = 1;
  public static final TimeUnit FIELD_CACHE_EXPIRY_UNIT = TimeUnit.MINUTES;
  public static final int FIELD_CACHE_MAXIMUM_SIZE = 32;
  
  public static final int LOOKUP_CACHE_EXPIRY_DURATION = 10;
  public static final TimeUnit LOOKUP_CACHE_EXPIRY_UNIT = TimeUnit.MINUTES;
  public static final int LOOKUP_CACHE_MAX_SIZE = 100;

  public static final int DOC_BUF_SIZE = 256;
  public static final int DEFAULT_BITSET_SIZE = 1 << 10;
  
  private LoadingCache<String, String[]> fieldDataCache;
  private LoadingCache<String, OpenBitSet> lookupCache;
  private IndexReader reader;
  
  public TermDocsLoader(final String field) {
    super(field);
    // Cache all values of a column in the dimension table in an array
    // Size of the array = table size.
    fieldDataCache = CacheBuilder.newBuilder()
        .maximumSize(FIELD_CACHE_MAXIMUM_SIZE)
        .expireAfterAccess(FIELD_CACHE_EXPIRY_DURATION, FIELD_CACHE_EXPIRY_UNIT)
        .build(new CacheLoader<String, String[]>(){
          @Override
          public String[] load(String columnToLoad) throws Exception {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Caching field data for column {}", columnToLoad);
            }
            return FieldCache.DEFAULT.getStrings(reader, columnToLoad);
          }
        });
    
    // Cache result of lookup by a given value of the foreign key joining this table with some other
    // lookup results are cached inside bitsets to save space.
    lookupCache = CacheBuilder.newBuilder()
        .maximumSize(LOOKUP_CACHE_MAX_SIZE)
        .expireAfterAccess(LOOKUP_CACHE_EXPIRY_DURATION, LOOKUP_CACHE_EXPIRY_UNIT)
        .build(new CacheLoader<String, OpenBitSet>(){

          @Override
          public OpenBitSet load(String lookupKey) throws Exception {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Caching lookup doc set for field '{}', value '{}'", field, lookupKey);
            }
            
            Term searchTerm = new Term(field, lookupKey);
            TermDocs docs = reader.termDocs(searchTerm);
            int docBuf[] = new int[DOC_BUF_SIZE];
            int freqBuf[] = new int[DOC_BUF_SIZE];
            
            int readDocs = 0;
            OpenBitSet docset = new OpenBitSet(DEFAULT_BITSET_SIZE);
            while ((readDocs = docs.read(docBuf, freqBuf)) > 0) {
              for (int i = 0; i < readDocs; i++) {
                docset.set(docBuf[i]);
              }
            }
            
            return docset;
          }
          
        });
  }
  
  public void setIndexReader(IndexReader idxReader) {
    this.reader = idxReader;
  }

  @Override
  public boolean load(String lookupKey, String columnToLoad, JoinCondition cond, List<Object> result) {
    return loadFromIndex(lookupKey, columnToLoad, cond, result);
  }

  public boolean loadFromIndex(String lookupKey, String columnToLoad, JoinCondition cond, List<Object> values) {
    // Assume one index per table.
    try {
      String terms[] = null;
      int added = 0;
      
      DocIdSetIterator docs = lookupCache.get(lookupKey).iterator();
      int rowid = -1;
      while ((rowid = docs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        // Check if data field cache loaded for this column
        if (terms == null) {
          terms = fieldDataCache.get(columnToLoad);
        }
        String val = terms[rowid];
        boolean add = false;
        if (cond != null) {
          // TODO Condition should ideally evaluate the entire row, i.e. the Lucene document
          add = cond.evaluate(val);
        } else {
          add = true;
        }

        if (add) {
          added ++;
          values.add(val);
        }
      }
      return added > 0;
    } catch (IOException e) {
      LOG.error("Error getting term docs", e);
      return false;
    } catch (ExecutionException e) {
      LOG.error("Error populating cache", e);
      return false;
    }
  }
}
