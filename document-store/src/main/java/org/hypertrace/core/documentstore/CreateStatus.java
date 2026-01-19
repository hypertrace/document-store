package org.hypertrace.core.documentstore;

/** Status of a document create operation. */
public enum CreateStatus {
  /** Document created successfully with all fields. */
  SUCCESS,

  /** Document created but some fields were skipped (didn't match schema). */
  PARTIAL,

  /** Document was intentionally not created due to IGNORE_DOCUMENT strategy. */
  IGNORED,

  /** Operation failed (no valid columns found or other error). */
  FAILED
}
