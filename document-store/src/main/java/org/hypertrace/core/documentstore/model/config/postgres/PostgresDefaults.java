package org.hypertrace.core.documentstore.model.config.postgres;

import java.time.Duration;

public interface PostgresDefaults {
  int DEFAULT_PORT = 5432;

  String DEFAULT_USER = "postgres";
  String DEFAULT_PASSWORD = "postgres";
  String DEFAULT_DB_NAME = "postgres";

  int DEFAULT_MAX_CONNECTION_ATTEMPTS = 200;
  Duration DEFAULT_CONNECTION_RETRY_BACKOFF = Duration.ofSeconds(5);
}
