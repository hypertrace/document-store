package org.hypertrace.core.documentstore.postgres;

import com.typesafe.config.Optional;
import java.time.Duration;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@NoArgsConstructor
public class PostgresConnectionPoolConfig {
  @Optional private int maxConnections = PostgresDefaults.DEFAULT_MAX_CONNECTIONS;
  @Optional private Duration maxWaitTime = PostgresDefaults.DEFAULT_MAX_WAIT_TIME;

  @Optional
  private Duration removeAbandonedTimeout = PostgresDefaults.DEFAULT_REMOVE_ABANDONED_TIMEOUT;
}
