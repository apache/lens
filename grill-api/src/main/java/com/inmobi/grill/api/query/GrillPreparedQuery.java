package com.inmobi.grill.api.query;

import java.util.Date;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.inmobi.grill.api.GrillConf;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class GrillPreparedQuery {
  @XmlElement @Getter private QueryPrepareHandle prepareHandle;
  @XmlElement @Getter private String userQuery;
  @XmlElement @Getter private Date preparedTime;
  @XmlElement @Getter private String preparedUser;
  @XmlElement @Getter private String selectedDriverClassName;
  @XmlElement @Getter private String driverQuery;
  @XmlElement @Getter private GrillConf conf;
}
