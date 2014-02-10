package com.inmobi.grill.common;

import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;

@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class StringList {
  @XmlElementWrapper @Getter @Setter private List<String> elements;
}
