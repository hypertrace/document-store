package org.hypertrace.core.documentstore.utils;

import java.io.IOException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.CreateResult;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.Filter.Op;
import org.hypertrace.core.documentstore.SingleValueKey;
import org.hypertrace.core.documentstore.UpdateResult;

public final class CreateUpdateTestThread extends Thread {
  private Collection collection;
  private int testValue;
  private CreateResult createTestResult;
  private UpdateResult updateTestResult;
  private SingleValueKey documentKey;
  private Operation operation;

  public enum Operation {
    CREATE,
    UPDATE
  };

  public static final String SUCCESS = "success";
  public static final String FAILURE = "failure";

  public CreateUpdateTestThread(
      Collection collection, SingleValueKey documentKey, int testValue, Operation operation) {
    this.collection = collection;
    this.testValue = testValue;
    this.documentKey = documentKey;
    this.operation = operation;
  }

  public int getTestValue() {
    return testValue;
  }

  public boolean getTestResult() {
    boolean success = false;
    switch (operation) {
      case CREATE:
        if (createTestResult != null) {
          success = createTestResult.isSucceed();
        }
        break;
      case UPDATE:
        if (updateTestResult != null) {
          success = updateTestResult.getUpdatedCount() > 0;
        }
        break;
      default:
        break;
    }
    return success;
  }

  @Override
  public void run() {
    try {
      switch (operation) {
        case CREATE:
          createRun();
          break;
        case UPDATE:
          updateRun();
          break;
        default:
          break;
      }
    } catch (Exception e) {
      // exception is consider as failure
    }
  }

  private void updateRun() throws IOException {
    Document document =
        Utils.createDocument(
            ImmutablePair.of("id", documentKey.getValue()),
            ImmutablePair.of("name", String.format("thread-%s", this.testValue)),
            ImmutablePair.of("size", this.testValue));

    // do conditional update
    Filter condition = new Filter(Op.EQ, "size", this.testValue);
    updateTestResult = collection.update(documentKey, document, condition);
  }

  private void createRun() throws IOException {
    Document document =
        Utils.createDocument(
            ImmutablePair.of("id", documentKey.getValue()),
            ImmutablePair.of("name", String.format("thread-%s", this.testValue)),
            ImmutablePair.of("size", this.testValue));
    createTestResult = collection.create(documentKey, document);
  }
}
