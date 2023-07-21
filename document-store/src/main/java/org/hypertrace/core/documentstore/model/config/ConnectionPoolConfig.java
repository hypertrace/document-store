package org.hypertrace.core.documentstore.model.config;

import java.time.Duration;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder
@Accessors(fluent = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ConnectionPoolConfig {
  @Nonnull @Nonnegative @Builder.Default Integer maxConnections = 1;

  // Time duration to wait for obtaining a connection from the pool
  @Nonnull @Builder.Default Duration connectionAccessTimeout = Duration.ofSeconds(5);

  // Time duration to wait for surrendering an idle connection back to the pool
  @Nonnull @Builder.Default Duration connectionSurrenderTimeout = Duration.ofSeconds(60);
}
