package com.inmobi.grill.query;

import java.util.Date;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.inmobi.grill.common.Priority;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class GrillQuery {

  @XmlElement @Getter private QueryHandle queryHandle;
  @XmlElement @Getter private String userQuery;
  @XmlElement @Getter private Date submissionTime;
  @XmlElement @Getter private String submittedUser;
  @XmlElement @Getter private Priority priority;
  @XmlElement @Getter private boolean isPersistent;
  @XmlElement @Getter private String selectedDriverClassName;
  @XmlElement @Getter private String driverQuery;
  @XmlElement @Getter private QueryStatus status;
  @XmlElement @Getter private String resultSetPath;
  @XmlElement @Getter private String driverOpHandle;
}