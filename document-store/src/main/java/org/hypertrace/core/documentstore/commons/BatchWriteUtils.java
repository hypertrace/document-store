package org.hypertrace.core.documentstore.commons;

import java.sql.Statement;

/** Shared helpers for validating JDBC batch write outcomes. */
public final class BatchWriteUtils {

  private BatchWriteUtils() {}

  /**
   * Returns true when every batch entry completed without {@link Statement#EXECUTE_FAILED} and the
   * result length matches the number of operations submitted.
   *
   * <p>{@link Statement#SUCCESS_NO_INFO} (-2) and positive update counts are treated as success.
   */
  public static boolean isBatchFullySuccessful(final int[] updateCounts, final int expectedSize) {
    if (updateCounts == null || updateCounts.length != expectedSize) {
      return false;
    }
    for (final int count : updateCounts) {
      if (count == Statement.EXECUTE_FAILED) {
        return false;
      }
    }
    return true;
  }
}
