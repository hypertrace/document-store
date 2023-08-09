package org.hypertrace.core.documentstore.model.config.mongo;

import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.hypertrace.core.documentstore.model.config.mongo.MongoDefaults.DEFAULT_ENDPOINT;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterSettings;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.experimental.NonFinal;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig;
import org.hypertrace.core.documentstore.model.config.ConnectionCredentials;
import org.hypertrace.core.documentstore.model.config.Endpoint;

@Value
@NonFinal
@Accessors(fluent = true)
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class MongoConnectionConfig extends ConnectionConfig {
  @NonNull String applicationName;
  @Nullable String replicaSetName;

  public MongoConnectionConfig(
      @NonNull final List<Endpoint> endpoints,
      @Nullable final String database,
      @Nullable final ConnectionCredentials credentials,
      @NonNull final String applicationName,
      @Nullable final String replicaSetName) {
    super(
        ensureAtLeastOneEndpoint(endpoints),
        getDatabaseOrDefault(database),
        getCredentialsOrDefault(credentials, database));
    this.applicationName = applicationName;
    this.replicaSetName = replicaSetName;
  }

  public MongoClientSettings toSettings() {
    final MongoClientSettings.Builder settingsBuilder =
        MongoClientSettings.builder().applicationName(applicationName()).retryWrites(true);

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

    final ConnectionCredentials credentials = credentials();
    if (credentials != null) {
      final MongoCredential credential =
          MongoCredential.createCredential(
              credentials.username(),
              credentials.authDatabase().orElseThrow(),
              credentials.password().toCharArray());
      settingsBuilder.credential(credential);
    }

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
    if (credentials == null || ConnectionCredentials.builder().build().equals(credentials)) {
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
}
