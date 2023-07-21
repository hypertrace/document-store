package org.hypertrace.core.documentstore.model.config;

import com.typesafe.config.Config;
import javax.annotation.Nonnull;
import lombok.Value;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig.ConnectionConfigBuilder;
import org.hypertrace.core.documentstore.model.config.ConnectionCredentials.ConnectionCredentialsBuilder;
import org.hypertrace.core.documentstore.model.config.ConnectionPoolConfig.ConnectionPoolConfigBuilder;

@Value
public class TypesafeConfigConnectionConfigExtractor {
  @Nonnull Config config;
  ConnectionConfigBuilder connectionConfigBuilder;
  ConnectionCredentialsBuilder connectionCredentialsBuilder;
  ConnectionPoolConfigBuilder connectionPoolConfigBuilder;

  private TypesafeConfigConnectionConfigExtractor(
      @Nonnull final Config config, @Nonnull final String typeKey) {
    this.config = config.getConfig(typeKey);
    this.connectionConfigBuilder = ConnectionConfig.builder().type(config.getString(typeKey));
    this.connectionCredentialsBuilder = ConnectionCredentials.builder();
    this.connectionPoolConfigBuilder = ConnectionPoolConfig.builder();
  }

  private TypesafeConfigConnectionConfigExtractor(
      @Nonnull final Config config, @Nonnull final DatabaseType type) {
    this.config = config;
    this.connectionConfigBuilder = ConnectionConfig.builder().type(type);
    this.connectionCredentialsBuilder = ConnectionCredentials.builder();
    this.connectionPoolConfigBuilder = ConnectionPoolConfig.builder();
  }

  public static TypesafeConfigConnectionConfigExtractor from(
      final Config config, final String typeKey) {
    return new TypesafeConfigConnectionConfigExtractor(config, typeKey);
  }

  public static TypesafeConfigConnectionConfigExtractor from(
      final Config config, final DatabaseType type) {
    return new TypesafeConfigConnectionConfigExtractor(config, type);
  }

  public TypesafeConfigConnectionConfigExtractor hostKey(@Nonnull final String key) {
    if (config.hasPath(key)) {
      connectionConfigBuilder.host(config.getString(key));
    }
    return this;
  }

  public TypesafeConfigConnectionConfigExtractor portKey(@Nonnull final String key) {
    if (config.hasPath(key)) {
      connectionConfigBuilder.port(config.getInt(key));
    }
    return this;
  }

  public TypesafeConfigConnectionConfigExtractor usernameKey(@Nonnull final String key) {
    if (config.hasPath(key)) {
      connectionCredentialsBuilder.username(config.getString(key));
    }
    return this;
  }

  public TypesafeConfigConnectionConfigExtractor passwordKey(@Nonnull final String key) {
    if (config.hasPath(key)) {
      connectionCredentialsBuilder.password(config.getString(key));
    }
    return this;
  }

  public TypesafeConfigConnectionConfigExtractor authDatabaseKey(@Nonnull final String key) {
    if (config.hasPath(key)) {
      connectionCredentialsBuilder.authDatabase(config.getString(key));
    }
    return this;
  }

  public TypesafeConfigConnectionConfigExtractor databaseKey(@Nonnull final String key) {
    if (config.hasPath(key)) {
      connectionConfigBuilder.database(config.getString(key));
    }
    return this;
  }

  public TypesafeConfigConnectionConfigExtractor applicationNameKey(@Nonnull final String key) {
    if (config.hasPath(key)) {
      connectionConfigBuilder.applicationName(config.getString(key));
    }
    return this;
  }

  public TypesafeConfigConnectionConfigExtractor poolMaxConnectionsKey(@Nonnull final String key) {
    if (config.hasPath(key)) {
      connectionPoolConfigBuilder.maxConnections(config.getInt(key));
    }
    return this;
  }

  public TypesafeConfigConnectionConfigExtractor poolConnectionAccessTimeoutKey(
      @Nonnull final String key) {
    if (config.hasPath(key)) {
      connectionPoolConfigBuilder.connectionAccessTimeout(config.getDuration(key));
    }
    return this;
  }

  public TypesafeConfigConnectionConfigExtractor poolConnectionSurrenderTimeoutKey(
      @Nonnull final String key) {
    if (config.hasPath(key)) {
      connectionPoolConfigBuilder.connectionSurrenderTimeout(config.getDuration(key));
    }
    return this;
  }

  public ConnectionConfig extract() {
    return connectionConfigBuilder.credentials(connectionCredentialsBuilder.build()).build();
  }
}
