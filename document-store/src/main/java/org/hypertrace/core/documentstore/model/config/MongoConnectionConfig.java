package org.hypertrace.core.documentstore.model.config;

import com.google.common.base.Preconditions;
import javax.annotation.Nonnegative;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;
import org.hypertrace.core.documentstore.model.config.mongo.MongoDefaults;

@Value
@Builder
@Accessors(fluent = true)
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class MongoConnectionConfig extends ConnectionConfig {
  @NonNull String host;

  @Default @NonNull @Nonnegative Integer port = 27017;

  @NonNull ConnectionCredentials credentials;

  @Default @NonNull String database = MongoDefaults.DEFAULT_DB_NAME;

  @NonNull String applicationName;

  @SuppressWarnings("unused")
  class MongoConnectionConfigBuilder {
    MongoConnectionConfig build() {
      final ConnectionCredentials updatedCredentials;
      if (!credentials.username().isBlank() && credentials.authDatabase().isEmpty()) {
        updatedCredentials = credentials.toBuilder().authDatabase(MongoDefaults.DEFAULT_DB_NAME).build();
      } else {
        updatedCredentials = credentials;
      }

      return new MongoConnectionConfig(host, port, updatedCredentials, database, applicationName);
    }
  }
}
