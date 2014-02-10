package com.inmobi.grill.query;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;


@XmlRootElement
@XmlSeeAlso({QueryHandle.class, QueryPrepareHandle.class,
  QueryHandleWithResultSet.class, com.inmobi.grill.query.QueryPlan.class})
public abstract class QuerySubmitResult {

}
