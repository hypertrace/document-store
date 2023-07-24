package org.hypertrace.core.documentstore.model.config.mongo;

import static org.hypertrace.core.documentstore.model.config.DatabaseType.MONGO;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
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

@Value
@NonFinal
@Accessors(fluent = true)
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class MongoConnectionConfig extends ConnectionConfig {
  @NonNull String applicationName;

  public MongoConnectionConfig(
      @NonNull final String host,
      @Nullable final Integer port,
      @Nullable final String database,
      @Nullable final ConnectionCredentials credentials,
      @NonNull final String applicationName) {
    super(
        MONGO,
        host,
        getPortOrDefault(port),
        getDatabaseOrDefault(database),
        getCredentialsOrDefault(credentials, database));
    this.applicationName = applicationName;
  }

  public ConnectionString toConnectionString() {
    return new ConnectionString(String.format("mongodb://%s:%d/%s", host(), port(), database()));
  }

  public MongoClientSettings toSettings() {
    final MongoClientSettings.Builder settingsBuilder =
        MongoClientSettings.builder()
            .applyConnectionString(toConnectionString())
            .applicationName(applicationName())
            .retryWrites(true);

    final ConnectionCredentials credentials = credentials();
    if (credentials != null) {
      settingsBuilder.credential(
          MongoCredential.createCredential(
              credentials.username(),
              credentials.authDatabase().orElseThrow(),
              credentials.password().toCharArray()));
    }

    return settingsBuilder.build();
  }

  @NonNull
  private static Integer getPortOrDefault(@Nullable final Integer port) {
    return Optional.ofNullable(port).orElse(MongoDefaults.DEFAULT_PORT);
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
}
