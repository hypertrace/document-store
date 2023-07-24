package org.hypertrace.core.documentstore.model.config.postgres;

import static java.util.function.Predicate.not;
import static org.hypertrace.core.documentstore.model.config.DatabaseType.POSTGRES;

import java.util.Optional;
import java.util.Properties;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.experimental.NonFinal;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig;
import org.hypertrace.core.documentstore.model.config.ConnectionCredentials;
import org.hypertrace.core.documentstore.model.config.ConnectionPoolConfig;
import org.postgresql.PGProperty;

@Value
@NonFinal
@Accessors(fluent = true)
@ToString(callSuper = true)
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

  @NonNull String applicationName;
  @NonNull ConnectionPoolConfig connectionPoolConfig;

  public PostgresConnectionConfig(
      @NonNull final String host,
      @Nullable final Integer port,
      @Nullable final String database,
      @Nullable final ConnectionCredentials credentials,
      @NonNull final String applicationName,
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

  public Properties buildProperties() {
    final Properties properties = new Properties();
    final ConnectionCredentials credentials = credentials();

    if (credentials != null) {
      properties.setProperty(PGProperty.USER.getName(), credentials.username());
      properties.setProperty(PGProperty.PASSWORD.getName(), credentials.password());
    }

    properties.setProperty(PGProperty.APPLICATION_NAME.getName(), applicationName());

    return properties;
  }

  @NonNull
  private static Integer getPortOrDefault(@Nullable final Integer port) {
    return Optional.ofNullable(port).orElse(PostgresDefaults.DEFAULT_PORT);
  }

  @NonNull
  private static String getDatabaseOrDefault(@Nullable final String database) {
    return Optional.ofNullable(database).orElse(PostgresDefaults.DEFAULT_DB_NAME);
  }

  @NonNull
  private static ConnectionCredentials getCredentialsOrDefault(
      @Nullable final ConnectionCredentials credentials) {
    return Optional.ofNullable(credentials)
        .filter(not(ConnectionCredentials.builder().build()::equals))
        .orElse(DEFAULT_CREDENTIALS);
  }

  @NonNull
  private ConnectionPoolConfig getConnectionPoolConfigOrDefault(
      @Nullable final ConnectionPoolConfig connectionPoolConfig) {
    return Optional.ofNullable(connectionPoolConfig)
        .filter(not(ConnectionPoolConfig.builder().build()::equals))
        .orElse(DEFAULT_CONNECTION_POOL_CONFIG);
  }
}
