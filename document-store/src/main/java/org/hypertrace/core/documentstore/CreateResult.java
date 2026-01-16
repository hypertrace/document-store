package org.hypertrace.core.documentstore;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;

/*
 * Represent the result object for CREATE operation of document store APIs.
 * */
@AllArgsConstructor
@Getter
public class CreateResult {
  private boolean isSucceed;
  private boolean onRetry;
  private List<String> skippedFields;

  public CreateResult(boolean isSucceed) {
    this.isSucceed = isSucceed;
  }

  public boolean isPartial() {
    return !skippedFields.isEmpty();
  }
}
