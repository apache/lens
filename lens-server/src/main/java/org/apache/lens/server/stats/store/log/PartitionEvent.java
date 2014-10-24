package org.apache.lens.server.stats.store.log;

import lombok.Getter;

import java.util.Map;

import org.apache.lens.server.api.events.LensEvent;

/**
 * Event class which encapsulates the partition information.
 */
public class PartitionEvent extends LensEvent {

  /** The event name. */
  private final String eventName;

  /** The part map. */
  private final Map<String, String> partMap;

  /** The class name. */
  @Getter
  private final String className;

  /**
   * Instantiates a new partition event.
   *
   * @param eventName
   *          the event name
   * @param partMap
   *          the part map
   * @param className
   *          the class name
   */
  public PartitionEvent(String eventName, Map<String, String> partMap, String className) {
    super(System.currentTimeMillis());
    this.eventName = eventName;
    this.partMap = partMap;
    this.className = className;
  }

  /**
   * Gets the statistics event for which partition event was raised
   *
   * @return name of the event class.
   */
  public String getEventName() {
    return eventName;
  }

  /**
   * Gets the partition map with location of log files to partition key.
   *
   * @return partition map with partition name to log file location.
   */
  public Map<String, String> getPartMap() {
    return partMap;
  }

  @Override
  public String getEventId() {
    return "partition event";
  }
}
