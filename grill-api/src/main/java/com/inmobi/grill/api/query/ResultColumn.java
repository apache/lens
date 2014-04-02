package com.inmobi.grill.api.query;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ResultColumn {

  @XmlElement @Getter private String name;
  @XmlElement @Getter private ResultColumnType type;

  public ResultColumn(String name, String type) {
    this(name, ResultColumnType.valueOf(type));
  }

  @Override
  public String toString() {
    return new StringBuilder(name).append(':').append(type).toString();
  }
}