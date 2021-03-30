package org.hypertrace.core.documentstore.utils;

import java.io.IOException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.DocStoreResult;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.Filter.Op;
import org.hypertrace.core.documentstore.SingleValueKey;

public class CreateUpdateTestThread extends Thread {
  private Collection collection;
  private int testValue;
  private DocStoreResult testResult;
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
    this.testResult = DocStoreResult.Builder.newBuilder().build();
  }

  public int getTestValue() {
    return testValue;
  }

  public DocStoreResult getTestResult() {
    return testResult;
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
    testResult = collection.update(documentKey, document, condition);
  }

  private void createRun() throws IOException {
    Document document =
        Utils.createDocument(
            ImmutablePair.of("id", documentKey.getValue()),
            ImmutablePair.of("name", String.format("thread-%s", this.testValue)),
            ImmutablePair.of("size", this.testValue));
    testResult = collection.create(documentKey, document);
  }
}
