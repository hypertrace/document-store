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
public class PostgresConfig {
  private final PostgresConnectionStringGetter connectionStringGetter =
      new PostgresConnectionStringGetter(this);

  @Optional private String host;
  @Optional private int port;

  @Optional @ToString.Exclude private String user = PostgresDefaults.DEFAULT_USER;
  @Optional @ToString.Exclude private String password = PostgresDefaults.DEFAULT_PASSWORD;

  @Optional private String database = PostgresDefaults.DEFAULT_DB_NAME;
  @Optional private String url;

  @Optional private int maxConnectionAttempts = PostgresDefaults.DEFAULT_MAX_CONNECTION_ATTEMPTS;

  @Optional
  private Duration connectionRetryBackoff = PostgresDefaults.DEFAULT_CONNECTION_RETRY_BACKOFF;

  @Optional
  private PostgresConnectionPoolConfig connectionPool = new PostgresConnectionPoolConfig();

  public String getConnectionString() {
    return connectionStringGetter.getConnectionString();
  }
}
