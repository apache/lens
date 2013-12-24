package com.inmobi.grill.api;

import java.util.UUID;

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
}
