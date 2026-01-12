package org.hypertrace.core.documentstore.model.config.postgres;

import static java.util.Collections.unmodifiableList;
import static java.util.function.Predicate.not;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import org.hypertrace.core.documentstore.model.config.DatabaseType;
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

  private static final Duration DEFAULT_SCHEMA_CACHE_EXPIRY = Duration.ofHours(24);
  private static final Duration DEFAULT_SCHEMA_REFRESH_COOLDOWN = Duration.ofMinutes(15);

  private static final String SCHEMA_CACHE_EXPIRY_MS_KEY = "schemaCacheExpiryMs";
  private static final String SCHEMA_REFRESH_COOLDOWN_MS_KEY = "schemaRefreshCooldownMs";

  @NonNull String applicationName;
  @NonNull ConnectionPoolConfig connectionPoolConfig;
  @NonNull Duration queryTimeout;
  @NonNull Duration schemaCacheExpiry;
  @NonNull Duration schemaRefreshCooldown;

  public static ConnectionConfigBuilder builder() {
    return ConnectionConfig.builder().type(DatabaseType.POSTGRES);
  }

  public PostgresConnectionConfig(
      @NonNull final List<Endpoint> endpoints,
      @Nullable final String database,
      @Nullable final ConnectionCredentials credentials,
      @NonNull final String applicationName,
      @Nullable final ConnectionPoolConfig connectionPoolConfig,
      @NonNull final Duration queryTimeout,
      @NonNull final Map<String, String> customParameters) {
    super(
        ensureSingleEndpoint(endpoints),
        getDatabaseOrDefault(database),
        getCredentialsOrDefault(credentials),
        customParameters);
    this.applicationName = applicationName;
    this.connectionPoolConfig = getConnectionPoolConfigOrDefault(connectionPoolConfig);
    this.queryTimeout = queryTimeout;
    this.schemaCacheExpiry = extractSchemaCacheExpiry(customParameters);
    this.schemaRefreshCooldown = extractSchemaRefreshCooldown(customParameters);
  }

  public String toConnectionString() {
    return String.format(
        "jdbc:postgresql://%s:%d/%s",
        endpoints().get(0).host(), endpoints().get(0).port(), database());
  }

  public Properties buildProperties() {
    final Properties properties = new Properties();
    final ConnectionCredentials credentials = credentials();

    if (credentials != null && !credentials.isEmpty()) {
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
        return fillInMissingPorts(endpoints);

      default:
        throw new IllegalArgumentException(
            String.format(
                "Cannot have more than 1 endpoint for Postgres. Found: %d", numEndpoints));
    }
  }

  private static List<Endpoint> fillInMissingPorts(final List<Endpoint> endpoints) {
    final List<Endpoint> updatedEndpoints = new ArrayList<>();

    for (final Endpoint endpoint : endpoints) {
      if (endpoint.port() == null) {
        updatedEndpoints.add(endpoint.toBuilder().port(PostgresDefaults.DEFAULT_PORT).build());
      } else {
        updatedEndpoints.add(endpoint);
      }
    }

    return unmodifiableList(updatedEndpoints);
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
    return Optional.ofNullable(connectionPoolConfig).orElse(ConnectionPoolConfig.builder().build());
  }

  @NonNull
  private static Duration extractSchemaCacheExpiry(final Map<String, String> customParameters) {
    return Optional.ofNullable(customParameters.get(SCHEMA_CACHE_EXPIRY_MS_KEY))
        .map(Long::parseLong)
        .map(Duration::ofMillis)
        .orElse(DEFAULT_SCHEMA_CACHE_EXPIRY);
  }

  @NonNull
  private static Duration extractSchemaRefreshCooldown(final Map<String, String> customParameters) {
    return Optional.ofNullable(customParameters.get(SCHEMA_REFRESH_COOLDOWN_MS_KEY))
        .map(Long::parseLong)
        .map(Duration::ofMillis)
        .orElse(DEFAULT_SCHEMA_REFRESH_COOLDOWN);
  }
}
