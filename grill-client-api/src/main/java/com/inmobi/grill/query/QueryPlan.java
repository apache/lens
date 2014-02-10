package com.inmobi.grill.query;

import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;

@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class QueryPlan extends QuerySubmitResult {
  @XmlElement @Getter private int numJoins = 0;
  @XmlElement @Getter private int numGbys = 0;
  @XmlElement @Getter private int numSels = 0;
  @XmlElement @Getter private int numSelDi = 0;
  @XmlElement @Getter private int numHaving = 0;
  @XmlElement @Getter private int numObys = 0;
  @XmlElement @Getter private int numAggrExprs = 0;
  @XmlElement @Getter private int numFilters = 0;
  @XmlElementWrapper @Getter private List<String> tablesQueried;
  @XmlElement @Getter private boolean hasSubQuery = false;
  @XmlElement @Getter private String execMode;
  @XmlElement @Getter private String scanMode;
  @XmlElementWrapper @Getter private Map<String, Double> tableWeights;
  @XmlElement @Getter private Double joinWeight;
  @XmlElement @Getter private Double gbyWeight;
  @XmlElement @Getter private Double filterWeight;
  @XmlElement @Getter private Double havingWeight;
  @XmlElement @Getter private Double obyWeight;
  @XmlElement @Getter private Double selectWeight;
  @Getter @Setter private QueryPrepareHandle prepareHandle;
  @XmlElement @Getter private String planString;
  @XmlElement @Getter private QueryCost queryCost;
}
