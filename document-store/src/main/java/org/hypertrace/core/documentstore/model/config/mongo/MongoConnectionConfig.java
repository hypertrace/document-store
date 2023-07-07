package org.hypertrace.core.documentstore.model.config.mongo;

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig;
import org.hypertrace.core.documentstore.model.config.ConnectionCredentials;
import org.hypertrace.core.documentstore.model.config.DatabaseType;

@Value
@SuperBuilder
@Accessors(fluent = true)
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class MongoConnectionConfig extends ConnectionConfig {

  public MongoConnectionConfig(final DatabaseType type, final String host, final Integer port, final ConnectionCredentials credentials, final String database, final String applicationName) {
    super(type, host, port, credentials, database, applicationName);
  }

  public static class MongoConnectionConfigBuilder
}
