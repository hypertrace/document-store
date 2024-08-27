package org.hypertrace.core.documentstore.model.options;

import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.AFTER_UPDATE;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class UpdateOptions {
  public static UpdateOptions DEFAULT_UPDATE_OPTIONS =
      UpdateOptions.builder()
          .returnDocumentType(AFTER_UPDATE)
          .missingDocumentStrategy(MissingDocumentStrategy.SKIP_UPDATES)
          .build();

  ReturnDocumentType returnDocumentType;
  MissingDocumentStrategy missingDocumentStrategy;

  public enum MissingDocumentStrategy {
    CREATE_USING_UPDATES,
    SKIP_UPDATES,
  }
}
