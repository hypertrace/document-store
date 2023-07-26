package org.hypertrace.core.documentstore.model;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig;
import org.hypertrace.core.documentstore.model.config.DatabaseType;

@Value
@Builder
@Accessors(fluent = true)
public class DatastoreConfig {
  @NonNull DatabaseType type;
  @NonNull ConnectionConfig connectionConfig;
}
