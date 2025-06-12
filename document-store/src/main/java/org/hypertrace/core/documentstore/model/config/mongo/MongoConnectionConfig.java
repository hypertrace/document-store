package org.hypertrace.core.documentstore.model.config.mongo;

import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.hypertrace.core.documentstore.model.config.mongo.MongoDefaults.DEFAULT_ENDPOINT;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoClientSettings.Builder;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.experimental.NonFinal;
import org.hypertrace.core.documentstore.model.config.AggregatePipelineMode;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig;
import org.hypertrace.core.documentstore.model.config.ConnectionCredentials;
import org.hypertrace.core.documentstore.model.config.ConnectionPoolConfig;
import org.hypertrace.core.documentstore.model.config.DatabaseType;
import org.hypertrace.core.documentstore.model.config.Endpoint;
import org.hypertrace.core.documentstore.model.options.DataFreshness;

@Value
@NonFinal
@Accessors(fluent = true)
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class MongoConnectionConfig extends ConnectionConfig {
  @NonNull String applicationName;
  @Nullable String replicaSetName;
  @NonNull ConnectionPoolConfig connectionPoolConfig;

  public static ConnectionConfigBuilder builder() {
    return ConnectionConfig.builder().type(DatabaseType.MONGO);
  }

  public MongoConnectionConfig(
      @NonNull final List<Endpoint> endpoints,
      @Nullable final String database,
      @Nullable final ConnectionCredentials credentials,
      @NonNull final String applicationName,
      @Nullable final String replicaSetName,
      @Nullable final ConnectionPoolConfig connectionPoolConfig,
      @NonNull final AggregatePipelineMode aggregationPipelineMode,
      @NonNull final DataFreshness dataFreshness,
      @NonNull final Duration queryTimeout,
      @NonNull final Map<String, String> customParameters) {
    super(
        ensureAtLeastOneEndpoint(endpoints),
        getDatabaseOrDefault(database),
        getCredentialsOrDefault(credentials, database),
        aggregationPipelineMode,
        dataFreshness,
        queryTimeout,
        customParameters);
    this.applicationName = applicationName;
    this.replicaSetName = replicaSetName;
    this.connectionPoolConfig = getConnectionPoolConfigOrDefault(connectionPoolConfig);
  }

  public MongoClientSettings toSettings() {
    final MongoClientSettings.Builder settingsBuilder =
        MongoClientSettings.builder()
            .applicationName(applicationName())
            .retryWrites(true)
            .retryReads(true);

    applyClusterSettings(settingsBuilder);
    applyConnectionPoolSettings(settingsBuilder);
    applyCredentialSettings(settingsBuilder);

    return settingsBuilder.build();
  }

  @NonNull
  private static List<Endpoint> ensureAtLeastOneEndpoint(final List<Endpoint> endpoints) {
    return endpoints.isEmpty() ? List.of(DEFAULT_ENDPOINT) : fillInMissingPorts(endpoints);
  }

  private static List<Endpoint> fillInMissingPorts(final List<Endpoint> endpoints) {
    final List<Endpoint> updatedEndpoints = new ArrayList<>();

    for (final Endpoint endpoint : endpoints) {
      if (endpoint.port() == null) {
        updatedEndpoints.add(endpoint.toBuilder().port(MongoDefaults.DEFAULT_PORT).build());
      } else {
        updatedEndpoints.add(endpoint);
      }
    }

    return unmodifiableList(updatedEndpoints);
  }

  @NonNull
  private static String getDatabaseOrDefault(@Nullable final String database) {
    return Optional.ofNullable(database).orElse(MongoDefaults.DEFAULT_DB_NAME);
  }

  @Nullable
  private static ConnectionCredentials getCredentialsOrDefault(
      @Nullable final ConnectionCredentials credentials, @Nullable final String database) {
    if (credentials == null || credentials.isEmpty()) {
      return null;
    }

    if (credentials.authDatabase().isPresent()) {
      return credentials;
    }

    return credentials.toBuilder().authDatabase(getDatabaseOrDefault(database)).build();
  }

  @SuppressWarnings("ConstantConditions")
  private static ServerAddress buildServerAddress(final Endpoint endpoint) {
    if (endpoint.port() != null) {
      return new ServerAddress(endpoint.host(), endpoint.port());
    }

    return new ServerAddress(endpoint.host());
  }

  @NonNull
  private ConnectionPoolConfig getConnectionPoolConfigOrDefault(
      @Nullable final ConnectionPoolConfig connectionPoolConfig) {
    return Optional.ofNullable(connectionPoolConfig).orElse(ConnectionPoolConfig.builder().build());
  }

  private void applyClusterSettings(final Builder settingsBuilder) {
    final List<ServerAddress> serverAddresses =
        endpoints().stream()
            .map(MongoConnectionConfig::buildServerAddress)
            .collect(toUnmodifiableList());
    final ClusterSettings clusterSettings =
        ClusterSettings.builder()
            .requiredReplicaSetName(replicaSetName)
            .hosts(serverAddresses)
            .build();
    settingsBuilder.applyToClusterSettings(builder -> builder.applySettings(clusterSettings));
  }

  private void applyConnectionPoolSettings(final Builder settingsBuilder) {
    final ConnectionPoolSettings connectionPoolSettings =
        ConnectionPoolSettings.builder()
            .maxSize(connectionPoolConfig.maxConnections())
            .maxWaitTime(
                connectionPoolConfig.connectionAccessTimeout().toMillis(), TimeUnit.MILLISECONDS)
            .maxConnectionIdleTime(
                connectionPoolConfig.connectionSurrenderTimeout().toMillis(), TimeUnit.MILLISECONDS)
            .build();

    settingsBuilder.applyToConnectionPoolSettings(
        builder -> builder.applySettings(connectionPoolSettings));
  }

  private void applyCredentialSettings(final Builder settingsBuilder) {
    final ConnectionCredentials credentials = credentials();
    if (credentials != null) {
      final MongoCredential credential =
          MongoCredential.createCredential(
              credentials.username(),
              credentials.authDatabase().orElseThrow(),
              credentials.password().toCharArray());
      settingsBuilder.credential(credential);
    }
  }
}
