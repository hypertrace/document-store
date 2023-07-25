package org.hypertrace.core.documentstore.model;

import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig;

@Value
@Builder
@Accessors(fluent = true)
public class DatastoreConfig {
  ConnectionConfig connectionConfig;
}
