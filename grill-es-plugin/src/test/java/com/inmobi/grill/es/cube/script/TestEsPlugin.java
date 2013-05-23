package com.inmobi.grill.es.cube.script;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.io.Files;

public class TestEsPlugin {
  TransportClient client;
  
  public static final String FACT_CSV = "src/test/resources/fact.csv";
  public static final String DIM_CSV = "src/test/resources/mydim.csv";
  
  //@BeforeClass
  public void createData() throws Exception {
    client = new TransportClient();
    client.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
    indexFact();
    indexMyDim();
  }

  private void indexMyDim() throws IOException {
    File factFile = new File(DIM_CSV);
    List<String> lines = Files.readLines(factFile, Charset.forName("UTF-8"));
    
    for (String line : lines) {
      if (line.startsWith("#")) {
        continue;
      }
      String toks[] = line.trim().split("\\,+");
      Map<String, Object> item = new HashMap<String, Object>();
      int i = 0;
      String id = toks[0];
      item.put("id", toks[i++].trim());
      item.put("dim2", toks[i++].trim());
      item.put("col1", toks[i++].trim());
      client.prepareIndex("mydim", "mydim", id).setSource(item).execute().actionGet();
    }
    
  }

  private void indexFact() throws IOException {
    File factFile = new File(FACT_CSV);
    List<String> lines = Files.readLines(factFile, Charset.forName("UTF-8"));
    
    for (String line : lines) {
      if (line.startsWith("#")) {
        continue;
      }
      String toks[] = line.trim().split("\\,+");
      Map<String, Object> item = new HashMap<String, Object>();
      int i = 0;
      String id = toks[0];
      item.put("id", toks[i++].trim());
      item.put("dim1", toks[i++].trim());
      item.put("dim2", toks[i++].trim());
      item.put("m1", toks[i++].trim());
      client.prepareIndex("fact", "fact", id).setSource(item).execute().actionGet();
    }
  }
  
  static String testParams = "{\n" + 
  		"            \"groupby\": [\"dim1\", \"col1\"],\n" + 
  		"\n" + 
  		"            \"aggregates\": [                   \n" + 
  		"                { \"name\": \"sum\", \"arguments\": [\"m1\"]},\n" + 
  		"                { \"name\": \"max\", \"arguments\": [\"m1\"]}\n" + 
  		"            ],\n" + 
  		"                \n" + 
  		"            \"measures\": [\"m1\"],\n" + 
  		"            \n" + 
  		"            \"main_table_fields\": [\"dim1\", \"dim2\"],\n" + 
  		"            \n" + 
  		"            \"join_chains\": {\n" + 
  		"                \"dim2\": {\n" + 
  		"                    \"join_column\": \"id\",\n" + 
  		"                    \"table\": \"mydim\",\n" + 
  		"                    \"type\": \"inner\",\n" + 
  		"                    \"get_columns\": [\"col1\"],\n" + 
  		"                    \"condition\": \"equal\"\n" + 
  		"                }\n" + 
  		"            }\n" + 
  		"        }";
  
  //@Test
  public void testEsPlugin() {
  }
  
  
  @AfterClass
  public void closeClient() {
    client.close();
  }
}
