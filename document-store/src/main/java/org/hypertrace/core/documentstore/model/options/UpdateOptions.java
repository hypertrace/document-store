package org.hypertrace.core.documentstore.model.options;

import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.AFTER_UPDATE;

import com.mongodb.client.model.FindOneAndUpdateOptions;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class UpdateOptions {
  public static UpdateOptions DEFAULT_UPDATE_OPTIONS =
      UpdateOptions.builder()
          .returnDocumentType(AFTER_UPDATE)
          .updateOptions(new com.mongodb.client.model.UpdateOptions())
          .findOneAndUpdateOptions(new FindOneAndUpdateOptions())
          .build();

  ReturnDocumentType returnDocumentType;
  com.mongodb.client.model.UpdateOptions updateOptions;
  FindOneAndUpdateOptions findOneAndUpdateOptions;
}
