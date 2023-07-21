package org.hypertrace.core.documentstore.postgres;

import java.util.Properties;
import org.hypertrace.core.documentstore.model.config.ConnectionCredentials;
import org.hypertrace.core.documentstore.model.config.PostgresConnectionConfig;
import org.postgresql.PGProperty;

public class PostgresPropertiesBuilder {
  public static Properties buildProperties(final PostgresConnectionConfig config) {
    final Properties properties = new Properties();
    final ConnectionCredentials credentials = config.credentials();

    if (credentials != null) {
      properties.setProperty(PGProperty.USER.getName(), credentials.username());
      properties.setProperty(PGProperty.PASSWORD.getName(), credentials.password());
    }

    properties.setProperty(PGProperty.APPLICATION_NAME.getName(), config.applicationName());

    return properties;
  }
}
