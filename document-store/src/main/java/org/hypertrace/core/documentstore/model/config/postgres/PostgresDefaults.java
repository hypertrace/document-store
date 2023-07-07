package org.hypertrace.core.documentstore.model.config.postgres;

import java.time.Duration;

public interface PostgresDefaults {

  int DEFAULT_MAX_CONNECTION_ATTEMPTS = 5;
  Duration DEFAULT_CONNECTION_RETRY_BACKOFF = Duration.ofSeconds(5);

  int DEFAULT_MAX_CONNECTIONS = 16;
  Duration DEFAULT_MAX_WAIT_TIME = Duration.ofSeconds(10);
  Duration DEFAULT_REMOVE_ABANDONED_TIMEOUT = Duration.ofSeconds(60);
}
