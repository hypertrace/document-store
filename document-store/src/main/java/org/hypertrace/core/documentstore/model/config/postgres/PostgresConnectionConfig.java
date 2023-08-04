package org.hypertrace.core.documentstore.model.config.postgres;

import static java.util.function.Predicate.not;

import java.util.List;
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
import org.hypertrace.core.documentstore.model.config.Endpoint;
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
      @NonNull final List<Endpoint> endpoints,
      @Nullable final String database,
      @Nullable final ConnectionCredentials credentials,
      @NonNull final String applicationName,
      @Nullable final ConnectionPoolConfig connectionPoolConfig) {
    super(
        ensureSingleEndpoint(endpoints),
        getDatabaseOrDefault(database),
        getCredentialsOrDefault(credentials));
    this.applicationName = applicationName;
    this.connectionPoolConfig = getConnectionPoolConfigOrDefault(connectionPoolConfig);
  }

  public String toConnectionString() {
    return String.format(
        "jdbc:postgresql://%s:%d/%s",
        endpoints().get(0).host(), endpoints().get(0).port(), database());
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
  private static List<Endpoint> ensureSingleEndpoint(@NonNull final List<Endpoint> endpoints) {
    final int numEndpoints = endpoints.size();

    switch (numEndpoints) {
      case 0:
        return List.of(Endpoint.builder().port(PostgresDefaults.DEFAULT_PORT).build());

      case 1:
        return endpoints;

      default:
        throw new IllegalArgumentException(
            String.format(
                "Cannot have more than 1 endpoint for Postgres. Found: %d", numEndpoints));
    }
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
