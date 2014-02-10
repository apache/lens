package com.inmobi.grill.common;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.UUID;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class GrillSessionHandle {
  @XmlElement @Getter private UUID publicId;
  @XmlElement @Getter private UUID secretId;

  private static final JAXBContext JAXB_CONTEXT;
  static {
    try {
        JAXB_CONTEXT = JAXBContext.newInstance(GrillSessionHandle.class);
    } catch (JAXBException e) {
        throw new RuntimeException(e);
    }
  }

  public static GrillSessionHandle valueOf(String sessionStr) {
    try {
      Unmarshaller unmarshaller = JAXB_CONTEXT.createUnmarshaller();
      return (GrillSessionHandle) unmarshaller.unmarshal(new StringReader(sessionStr));
    } catch (JAXBException e) {
      return null;
    }
  }

  @Override
  public String toString() {
      try {
          StringWriter stringWriter = new StringWriter();
          Marshaller marshaller = JAXB_CONTEXT.createMarshaller();
          marshaller.marshal(this, stringWriter);
          return stringWriter.toString();
      } catch (JAXBException e) {
          return "";
      }
  }

/*
  public GrillSessionHandle(SessionHandle sessionHandle) {
    this.publicId = sessionHandle.getHandleIdentifier().getPublicId();
    this.secretId = sessionHandle.getHandleIdentifier().getSecretId();
  }

  public SessionHandle getSessionHandle() {
    return new SessionHandle(new HandleIdentifier(publicId, secretId));
  }
*/
}
