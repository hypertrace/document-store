package org.hypertrace.core.documentstore.model.config;

import java.time.Duration;
import java.util.List;
import javax.annotation.Nonnegative;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.experimental.Accessors;

/**
 * See <a href="https://commons.apache.org/proper/commons-dbcp/configuration.html">DBCP2
 * Configuration</a> for details
 */
@Value
@Builder
@Accessors(fluent = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ConnectionPoolConfig {

  @NonNull @Nonnegative @Builder.Default Integer maxConnections = 16;

  // max idle connections as a percentage of max connections; 20% by default
  @NonNull @Builder.Default Integer maxIdlePercent = 20;

  // min idle connections as a percentage of max connections; 10% by default
  @NonNull @Builder.Default Integer minIdlePercent = 10;

  // Time duration to wait for obtaining a connection from the pool
  @NonNull @Builder.Default Duration connectionAccessTimeout = Duration.ofSeconds(10);

  // Time duration to wait for surrendering an idle connection back to the pool
  @NonNull @Builder.Default Duration connectionSurrenderTimeout = Duration.ofMinutes(5);

  @Nullable String validationQuery;

  @Nullable Duration validationQueryTimeout;

  @Nullable Boolean testOnCreate;

  // Setting this to false explictly since leaving it true by default has a non-trivial perf impact
  @NonNull @Builder.Default Boolean testOnBorrow = false;

  @Nullable Boolean testOnReturn;

  @Nullable Boolean testWhileIdle;

  @Nullable Duration timeBetweenEvictionRuns;

  @Nullable Integer numTestsPerEvictionRun;

  @Nullable Duration minEvictableIdleTime;

  @Nullable Duration softMinEvictableIdleTime;

  @Nullable Duration maxConnLifetime;

  @NonNull
  @Singular("connectionInitSql")
  List<String> connectionInitSqls;

  @Nullable Boolean lifo;
}
