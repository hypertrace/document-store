package org.hypertrace.core.documentstore.model.config.postgres;

import java.time.Duration;

public interface PostgresDefaults {
  String DEFAULT_USER = "postgres";
  String DEFAULT_PASSWORD = "postgres";
  String DEFAULT_DB_NAME = "postgres";

  int DEFAULT_MAX_CONNECTION_ATTEMPTS = 200;
  Duration DEFAULT_CONNECTION_RETRY_BACKOFF = Duration.ofSeconds(5);

  int DEFAULT_MAX_CONNECTIONS = 16;
  Duration DEFAULT_MAX_WAIT_TIME = Duration.ofSeconds(10);
  Duration DEFAULT_REMOVE_ABANDONED_TIMEOUT = Duration.ofSeconds(60);
}
