/*
 * 
 */
package org.apache.lens.api.query;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * The Class QueryResult.
 */
@XmlRootElement
@XmlSeeAlso({ PersistentQueryResult.class, InMemoryQueryResult.class })
/**
 * Instantiates a new query result.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class QueryResult {
}
