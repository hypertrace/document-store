package org.hypertrace.core.documentstore.model.config.postgres;

import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class CollectionConfig {

  /** Timestamp field configuration for auto-managed created/updated timestamps */
  @Nullable TimestampFieldsConfig timestampFields;

  public Optional<TimestampFieldsConfig> getTimestampFields() {
    return Optional.ofNullable(timestampFields);
  }

  public static CollectionConfig empty() {
    return CollectionConfig.builder().build();
  }
}
