package org.hypertrace.core.documentstore.model.config;

import java.time.Duration;
import javax.annotation.Nonnegative;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder
@Accessors(fluent = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ConnectionPoolConfig {
  @NonNull @Nonnegative @Builder.Default Integer maxConnections = 16;

  // max idle connections as a percentage of max connections; -1 means pin to maxConnections
  @NonNull @Builder.Default Integer maxIdlePercent = -1;

  // min idle connections as a percentage of max connections; -1 means pin to maxConnections
  @NonNull @Builder.Default Integer minIdlePercent = -1;

  // Time duration to wait for obtaining a connection from the pool
  @NonNull @Builder.Default Duration connectionAccessTimeout = Duration.ofSeconds(10);

  // Time duration to wait for surrendering an idle connection back to the pool
  @NonNull @Builder.Default Duration connectionSurrenderTimeout = Duration.ofMinutes(5);
}
