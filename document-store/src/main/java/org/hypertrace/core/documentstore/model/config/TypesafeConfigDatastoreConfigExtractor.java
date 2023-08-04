package org.hypertrace.core.documentstore.model.config;

import com.typesafe.config.Config;
import java.util.Optional;
import lombok.NonNull;
import lombok.Value;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig.ConnectionConfigBuilder;
import org.hypertrace.core.documentstore.model.config.ConnectionCredentials.ConnectionCredentialsBuilder;
import org.hypertrace.core.documentstore.model.config.ConnectionPoolConfig.ConnectionPoolConfigBuilder;
import org.hypertrace.core.documentstore.model.config.DatastoreConfig.DatastoreConfigBuilder;

@Value
public class TypesafeConfigDatastoreConfigExtractor {
  @NonNull Config config;
  DatastoreConfigBuilder datastoreConfigBuilder;
  ConnectionConfigBuilder connectionConfigBuilder;
  ConnectionCredentialsBuilder connectionCredentialsBuilder;
  ConnectionPoolConfigBuilder connectionPoolConfigBuilder;

  private TypesafeConfigDatastoreConfigExtractor(
      @NonNull final Config config, @NonNull final String typeKey) {
    this(
        config,
        Optional.of(typeKey)
            .filter(config::hasPath)
            .map(config::getString)
            .map(DatabaseType::getType)
            .orElseThrow(
                () -> new IllegalArgumentException("Missing or invalid type key: " + typeKey)));
  }

  private TypesafeConfigDatastoreConfigExtractor(
      @NonNull final Config config, @NonNull final DatabaseType type) {
    this.config = config;
    this.connectionConfigBuilder = ConnectionConfig.builder().type(type);
    this.connectionCredentialsBuilder = ConnectionCredentials.builder();
    this.connectionPoolConfigBuilder = ConnectionPoolConfig.builder();
    this.datastoreConfigBuilder = DatastoreConfig.builder().type(type);
  }

  public static TypesafeConfigDatastoreConfigExtractor from(
      @NonNull final Config config, @NonNull final String typeKey) {
    return new TypesafeConfigDatastoreConfigExtractor(config, typeKey);
  }

  public static TypesafeConfigDatastoreConfigExtractor from(
      @NonNull final Config config, @NonNull final DatabaseType type) {
    return new TypesafeConfigDatastoreConfigExtractor(config, type);
  }

  public TypesafeConfigDatastoreConfigExtractor hostKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionConfigBuilder.host(config.getString(key));
    }
    return this;
  }

  public TypesafeConfigDatastoreConfigExtractor portKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionConfigBuilder.port(config.getInt(key));
    }
    return this;
  }

  public TypesafeConfigDatastoreConfigExtractor usernameKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionCredentialsBuilder.username(config.getString(key));
    }
    return this;
  }

  public TypesafeConfigDatastoreConfigExtractor passwordKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionCredentialsBuilder.password(config.getString(key));
    }
    return this;
  }

  public TypesafeConfigDatastoreConfigExtractor authDatabaseKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionCredentialsBuilder.authDatabase(config.getString(key));
    }
    return this;
  }

  public TypesafeConfigDatastoreConfigExtractor databaseKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionConfigBuilder.database(config.getString(key));
    }
    return this;
  }

  public TypesafeConfigDatastoreConfigExtractor applicationNameKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionConfigBuilder.applicationName(config.getString(key));
    }
    return this;
  }

  public TypesafeConfigDatastoreConfigExtractor poolMaxConnectionsKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionPoolConfigBuilder.maxConnections(config.getInt(key));
    }
    return this;
  }

  public TypesafeConfigDatastoreConfigExtractor poolConnectionAccessTimeoutKey(
      @NonNull final String key) {
    if (config.hasPath(key)) {
      connectionPoolConfigBuilder.connectionAccessTimeout(config.getDuration(key));
    }
    return this;
  }

  public TypesafeConfigDatastoreConfigExtractor poolConnectionSurrenderTimeoutKey(
      @NonNull final String key) {
    if (config.hasPath(key)) {
      connectionPoolConfigBuilder.connectionSurrenderTimeout(config.getDuration(key));
    }
    return this;
  }

  public DatastoreConfig extract() {
    return datastoreConfigBuilder
        .connectionConfig(
            connectionConfigBuilder
                .connectionPoolConfig(connectionPoolConfigBuilder.build())
                .credentials(connectionCredentialsBuilder.build())
                .build())
        .build();
  }
}
