package com.inmobi.grill.query;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class InMemoryQueryResult extends QueryResult {

  @XmlElementWrapper @Getter
  private List<ResultRow> rows = new ArrayList<ResultRow>();
}
