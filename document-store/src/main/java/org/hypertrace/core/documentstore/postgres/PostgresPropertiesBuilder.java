package org.hypertrace.core.documentstore.postgres;

import java.util.Properties;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig;
import org.postgresql.PGProperty;

public class PostgresPropertiesBuilder {
  public static Properties buildProperties(final ConnectionConfig config) {
    final Properties properties = new Properties();
    properties.setProperty(PGProperty.USER.getName(), config.credentials().username());
    properties.setProperty(PGProperty.PASSWORD.getName(), config.credentials().password());
    properties.setProperty(PGProperty.APPLICATION_NAME.getName(), config.applicationName());

    return properties;
  }
}
