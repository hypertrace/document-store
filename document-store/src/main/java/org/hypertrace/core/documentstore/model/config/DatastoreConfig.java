package org.hypertrace.core.documentstore.model.config;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder
@Accessors(fluent = true)
public class DatastoreConfig {
  @NonNull DatabaseType type;
  @NonNull ConnectionConfig connectionConfig;
}
