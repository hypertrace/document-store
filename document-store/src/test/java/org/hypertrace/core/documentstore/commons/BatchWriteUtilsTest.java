package org.hypertrace.core.documentstore.commons;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Statement;
import org.junit.jupiter.api.Test;

class BatchWriteUtilsTest {

  @Test
  void isBatchFullySuccessful_allPositive_returnsTrue() {
    assertTrue(BatchWriteUtils.isBatchFullySuccessful(new int[] {1, 1, 2}, 3));
  }

  @Test
  void isBatchFullySuccessful_successNoInfo_returnsTrue() {
    assertTrue(
        BatchWriteUtils.isBatchFullySuccessful(
            new int[] {Statement.SUCCESS_NO_INFO, Statement.SUCCESS_NO_INFO}, 2));
  }

  @Test
  void isBatchFullySuccessful_executeFailed_returnsFalse() {
    assertFalse(BatchWriteUtils.isBatchFullySuccessful(new int[] {1, Statement.EXECUTE_FAILED}, 2));
  }

  @Test
  void isBatchFullySuccessful_lengthMismatch_returnsFalse() {
    assertFalse(BatchWriteUtils.isBatchFullySuccessful(new int[] {1}, 2));
  }

  @Test
  void isBatchFullySuccessful_null_returnsFalse() {
    assertFalse(BatchWriteUtils.isBatchFullySuccessful(null, 0));
  }
}
