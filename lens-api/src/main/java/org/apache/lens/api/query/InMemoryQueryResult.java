/*
 * 
 */
package org.apache.lens.api.query;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * The Class InMemoryQueryResult.
 */
@XmlRootElement
/**
 * Instantiates a new in memory query result.
 *
 * @param rows
 *          the rows
 */
@AllArgsConstructor
/**
 * Instantiates a new in memory query result.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class InMemoryQueryResult extends QueryResult {

  /** The rows. */
  @XmlElementWrapper
  @Getter
  private List<ResultRow> rows = new ArrayList<ResultRow>();
}
