package com.inmobi.grill.api;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.UUID;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hive.service.cli.HandleIdentifier;
import org.apache.hive.service.cli.SessionHandle;

@XmlRootElement
public class GrillSessionHandle {
  @XmlElement
  private UUID publicId;
  @XmlElement
  private UUID secretId;
  public GrillSessionHandle() {
  }

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

  public GrillSessionHandle(UUID publicId, UUID secretId) {
    this.publicId = publicId;
    this.secretId = secretId;
  }

  public GrillSessionHandle(SessionHandle sessionHandle) {
    this.publicId = sessionHandle.getHandleIdentifier().getPublicId();
    this.secretId = sessionHandle.getHandleIdentifier().getSecretId();
  }

  public SessionHandle getSessionHandle() {
    return new SessionHandle(new HandleIdentifier(publicId, secretId));
  }

  /**
   * @return the publicId
   */
  public UUID getPublicId() {
    return publicId;
  }

  /**
   * @return the secretId
   */
  public UUID getSecretId() {
    return secretId;
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
}
