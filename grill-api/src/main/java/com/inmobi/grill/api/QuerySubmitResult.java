package com.inmobi.grill.api;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;

@XmlRootElement
@XmlSeeAlso({QueryHandle.class, QueryPrepareHandle.class,
  QueryHandleWithResultSet.class, com.inmobi.grill.client.api.QueryPlan.class})
public abstract class QuerySubmitResult {

}
