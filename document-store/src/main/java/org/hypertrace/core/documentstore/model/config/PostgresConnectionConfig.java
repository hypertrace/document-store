package org.hypertrace.core.documentstore.model.config;

import javax.annotation.Nonnegative;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;
import org.hypertrace.core.documentstore.model.config.postgres.PostgresDefaults;

@Value
@Builder
@Accessors(fluent = true)
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public class PostgresConnectionConfig extends ConnectionConfig {
  @NonNull String host;

  @Default @NonNull @Nonnegative Integer port = 5432;

  @Default @NonNull ConnectionCredentials credentials = ConnectionCredentials.builder()
      .username(PostgresDefaults.DEFAULT_USER)
      .password(PostgresDefaults.DEFAULT_PASSWORD)
      .build();

  @Default @NonNull String database = PostgresDefaults.DEFAULT_DB_NAME;

  @NonNull String applicationName;

  @NonNull @Default
  ConnectionPoolConfig connectionPoolConfig = ConnectionPoolConfig.builder()
      .maxConnections(PostgresDefaults.DEFAULT_MAX_CONNECTIONS)
      .connectionAccessTimeout(PostgresDefaults.DEFAULT_MAX_WAIT_TIME)
      .connectionSurrenderTimeout(PostgresDefaults.DEFAULT_REMOVE_ABANDONED_TIMEOUT)
      .build();
}
