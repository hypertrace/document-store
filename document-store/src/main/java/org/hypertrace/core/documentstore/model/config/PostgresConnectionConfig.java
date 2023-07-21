package org.hypertrace.core.documentstore.model.config;

import static java.util.function.Predicate.not;
import static org.hypertrace.core.documentstore.model.config.DatabaseType.POSTGRES;

import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.Accessors;
import org.hypertrace.core.documentstore.model.config.postgres.PostgresDefaults;

@Value
@Accessors(fluent = true)
@EqualsAndHashCode(callSuper = true)
public class PostgresConnectionConfig extends ConnectionConfig {

  private static final ConnectionCredentials DEFAULT_CREDENTIALS =
      ConnectionCredentials.builder()
          .username(PostgresDefaults.DEFAULT_USER)
          .password(PostgresDefaults.DEFAULT_PASSWORD)
          .build();

  private static final ConnectionPoolConfig DEFAULT_CONNECTION_POOL_CONFIG =
      ConnectionPoolConfig.builder()
          .maxConnections(PostgresDefaults.DEFAULT_MAX_CONNECTIONS)
          .connectionAccessTimeout(PostgresDefaults.DEFAULT_MAX_WAIT_TIME)
          .connectionSurrenderTimeout(PostgresDefaults.DEFAULT_REMOVE_ABANDONED_TIMEOUT)
          .build();

  @Nonnull String applicationName;
  @Nonnull ConnectionPoolConfig connectionPoolConfig;

  public PostgresConnectionConfig(
      @Nonnull final String host,
      @Nullable final Integer port,
      @Nullable final String database,
      @Nullable final ConnectionCredentials credentials,
      @Nonnull final String applicationName,
      @Nullable final ConnectionPoolConfig connectionPoolConfig) {
    super(
        POSTGRES,
        host,
        getPortOrDefault(port),
        getDatabaseOrDefault(database),
        getCredentialsOrDefault(credentials));
    this.applicationName = applicationName;
    this.connectionPoolConfig = getConnectionPoolConfigOrDefault(connectionPoolConfig);
  }

  public String toConnectionString() {
    return String.format("jdbc:postgresql://%s:%d/%s", host(), port(), database());
  }

  @Nonnull
  private static Integer getPortOrDefault(@Nullable final Integer port) {
    return Optional.ofNullable(port).orElse(PostgresDefaults.DEFAULT_PORT);
  }

  @Nonnull
  private static String getDatabaseOrDefault(@Nullable final String database) {
    return Optional.ofNullable(database).orElse(PostgresDefaults.DEFAULT_DB_NAME);
  }

  @Nonnull
  private static ConnectionCredentials getCredentialsOrDefault(
      @Nullable final ConnectionCredentials credentials) {
    return Optional.ofNullable(credentials)
        .filter(not(ConnectionCredentials.builder().build()::equals))
        .orElse(DEFAULT_CREDENTIALS);
  }

  @Nonnull
  private ConnectionPoolConfig getConnectionPoolConfigOrDefault(
      @Nullable final ConnectionPoolConfig connectionPoolConfig) {
    return Optional.ofNullable(connectionPoolConfig)
        .filter(not(ConnectionPoolConfig.builder().build()::equals))
        .orElse(DEFAULT_CONNECTION_POOL_CONFIG);
  }
}
