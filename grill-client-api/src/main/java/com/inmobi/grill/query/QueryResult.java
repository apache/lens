package com.inmobi.grill.query;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@XmlRootElement
@XmlSeeAlso({PersistentQueryResult.class, InMemoryQueryResult.class})
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class QueryResult {
}
