package org.apache.lens.server.api.query;

import org.apache.lens.server.api.query.QueryContext;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Class to represent the Finished lens query which is serialized to database.
 */

/*
 * (non-Javadoc)
 * 
 * @see java.lang.Object#hashCode()
 */
@EqualsAndHashCode
/*
 * (non-Javadoc)
 * 
 * @see java.lang.Object#toString()
 */
@ToString
public class FinishedLensQuery {

  /** The handle. */
  @Getter
  @Setter
  private String handle;

  /** The user query. */
  @Getter
  @Setter
  private String userQuery;

  /** The submitter. */
  @Getter
  @Setter
  private String submitter;

  /** The submission time. */
  @Getter
  @Setter
  private long submissionTime;

  /** The start time. */
  @Getter
  @Setter
  private long startTime;

  /** The end time. */
  @Getter
  @Setter
  private long endTime;

  /** The result. */
  @Getter
  @Setter
  private String result;

  /** The status. */
  @Getter
  @Setter
  private String status;

  /** The metadata. */
  @Getter
  @Setter
  private String metadata;

  /** The rows. */
  @Getter
  @Setter
  private int rows;

  /** The error message. */
  @Getter
  @Setter
  private String errorMessage;

  /** The driver start time. */
  @Getter
  @Setter
  private long driverStartTime;

  /** The driver end time. */
  @Getter
  @Setter
  private long driverEndTime;

  /** The metadata class. */
  @Getter
  @Setter
  private String metadataClass;

  /** The query name. */
  @Getter
  @Setter
  private String queryName;

  /**
   * Instantiates a new finished lens query.
   */
  public FinishedLensQuery() {

  }

  /**
   * Instantiates a new finished lens query.
   *
   * @param ctx
   *          the ctx
   */
  public FinishedLensQuery(QueryContext ctx) {
    this.handle = ctx.getQueryHandle().toString();
    this.userQuery = ctx.getUserQuery();
    this.submitter = ctx.getSubmittedUser();
    this.submissionTime = ctx.getSubmissionTime();
    this.startTime = ctx.getLaunchTime();
    this.endTime = ctx.getEndTime();
    this.result = ctx.getResultSetPath();
    this.status = ctx.getStatus().getStatus().name();
    this.errorMessage = ctx.getStatus().getErrorMessage();
    this.driverStartTime = ctx.getDriverStatus().getDriverStartTime();
    this.driverEndTime = ctx.getDriverStatus().getDriverFinishTime();
    if (ctx.getQueryName() != null) {
      this.queryName = ctx.getQueryName().toLowerCase();
    }
  }

}
