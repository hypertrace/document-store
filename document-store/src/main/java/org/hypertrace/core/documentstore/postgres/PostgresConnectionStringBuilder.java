package org.hypertrace.core.documentstore.postgres;

import lombok.Getter;
import lombok.NonNull;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig;

@Getter
public class PostgresConnectionStringBuilder {
  private final String connectionString;

  PostgresConnectionStringBuilder(@NonNull final ConnectionConfig connectionConfig) {
    this.connectionString = buildConnectionString(connectionConfig);
  }

  private String buildConnectionString(final ConnectionConfig config) {
    return String.format(
        "jdbc:postgresql://%s:%d/%s", config.host(), config.port(), config.database());
  }
}
