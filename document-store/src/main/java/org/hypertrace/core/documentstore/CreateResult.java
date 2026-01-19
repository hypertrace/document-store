package org.hypertrace.core.documentstore;

import java.util.Collections;
import java.util.List;
import lombok.Getter;

/*
 * Represent the result object for CREATE operation of document store APIs.
 * */
@Getter
public class CreateResult {

  private final CreateStatus status;
  private final boolean onRetry;
  private final List<String> skippedFields;

  public CreateResult(CreateStatus status, boolean onRetry, List<String> skippedFields) {
    this.status = status;
    this.onRetry = onRetry;
    this.skippedFields = skippedFields != null ? skippedFields : Collections.emptyList();
  }

  public CreateResult(boolean isSuccess) {
    this(isSuccess ? CreateStatus.SUCCESS : CreateStatus.FAILED, false, Collections.emptyList());
  }

  public CreateResult(boolean isSucceed, boolean onRetry, List<String> skippedFields) {
    this(determineStatus(isSucceed, skippedFields), onRetry, skippedFields);
  }

  private static CreateStatus determineStatus(boolean isSucceed, List<String> skippedFields) {
    if (!isSucceed) {
      return CreateStatus.FAILED;
    }
    return (skippedFields != null && !skippedFields.isEmpty())
        ? CreateStatus.PARTIAL
        : CreateStatus.SUCCESS;
  }

  public boolean isSucceed() {
    return status == CreateStatus.SUCCESS || status == CreateStatus.PARTIAL;
  }

  public boolean isPartial() {
    return status == CreateStatus.PARTIAL;
  }

  public boolean isDocumentIgnored() {
    return status == CreateStatus.IGNORED;
  }
}
