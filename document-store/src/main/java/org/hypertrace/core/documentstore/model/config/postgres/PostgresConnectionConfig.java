package org.hypertrace.core.documentstore.model.config.postgres;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig;

@Value
@SuperBuilder
@Accessors(fluent = true)
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PostgresConnectionConfig extends ConnectionConfig {

  @NonNull
  @Builder.Default
  PostgresConnectionPoolConfig connectionPoolConfig = PostgresConnectionPoolConfig.builder().build();
}
