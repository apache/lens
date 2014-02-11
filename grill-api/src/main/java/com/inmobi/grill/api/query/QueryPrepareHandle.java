package com.inmobi.grill.api.query;

import java.util.UUID;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@EqualsAndHashCode(callSuper = false)
public class QueryPrepareHandle extends QuerySubmitResult {
  @XmlElement @Getter
  private UUID prepareHandleId;

  public static QueryPrepareHandle fromString(String handle) {
    return new QueryPrepareHandle(UUID.fromString(handle));
  }

  @Override
  public String toString() {
    return prepareHandleId.toString();
  }
}
