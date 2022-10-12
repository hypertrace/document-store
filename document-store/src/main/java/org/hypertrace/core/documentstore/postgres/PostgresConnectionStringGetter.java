package org.hypertrace.core.documentstore.postgres;

import com.google.common.base.Suppliers;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

public class PostgresConnectionStringGetter {
  private final PostgresConfig config;
  private final Supplier<String> connectionStringSupplier;

  PostgresConnectionStringGetter(@Nonnull final PostgresConfig config) {
    this.config = config;
    this.connectionStringSupplier = Suppliers.memoize(this::buildConnectionStringIfRequired);
  }

  String getConnectionString() {
    return connectionStringSupplier.get();
  }

  private String buildConnectionStringIfRequired() {
    return Optional.of(config)
        .map(PostgresConfig::getUrl)
        .or(() -> Optional.of(buildConnectionString(config)))
        .map(url -> appendDatabase(url, config))
        .orElseThrow();
  }

  private static String buildConnectionString(final PostgresConfig config) {
    return String.format("jdbc:postgresql://%s:%d/", config.getHost(), config.getPort());
  }

  private static String appendDatabase(
      final String baseConnectionString, final PostgresConfig config) {
    return baseConnectionString + config.getDatabase();
  }
}
