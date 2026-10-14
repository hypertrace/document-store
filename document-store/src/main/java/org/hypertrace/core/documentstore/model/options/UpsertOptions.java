package org.hypertrace.core.documentstore.model.options;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;

@Value
@Builder
public class UpsertOptions {
  public static UpsertOptions DEFAULT_UPSERT_OPTIONS =
      UpsertOptions.builder().returnDocuments(ReturnDocuments.AFTER).build();

  @Default ReturnDocuments returnDocuments = ReturnDocuments.AFTER;

  public enum ReturnDocuments {
    BEFORE,
    AFTER,
    BOTH,
  }
}
