package com.inmobi.grill.elasticsearch;

import org.elasticsearch.common.xcontent.XContentBuilder;

public class Document {
	private String id;
	private XContentBuilder document;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public XContentBuilder getDocument() {
		return document;
	}
	public void setDocument(XContentBuilder document) {
		this.document = document;
	}
	
	  // ElasticSearch 'type' for the event objects
	  public static final String EVENT_TYPE = "e";
	
	

}
