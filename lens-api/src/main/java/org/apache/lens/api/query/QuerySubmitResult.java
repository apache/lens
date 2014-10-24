/*
 * 
 */
package org.apache.lens.api.query;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;

/**
 * The Class QuerySubmitResult.
 */
@XmlRootElement
@XmlSeeAlso({ QueryHandle.class, QueryPrepareHandle.class, QueryHandleWithResultSet.class,
  org.apache.lens.api.query.QueryPlan.class })
public abstract class QuerySubmitResult {

}
