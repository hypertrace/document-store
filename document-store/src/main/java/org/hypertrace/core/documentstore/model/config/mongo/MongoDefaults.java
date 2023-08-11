package org.hypertrace.core.documentstore.model.config.mongo;

import com.mongodb.ServerAddress;
import org.hypertrace.core.documentstore.model.config.Endpoint;

public interface MongoDefaults {
  String DEFAULT_DB_NAME = "default_db";
  String ADMIN_DATABASE = "admin";
  Integer DEFAULT_PORT = ServerAddress.defaultPort();
  Endpoint DEFAULT_ENDPOINT =
      Endpoint.builder().host(ServerAddress.defaultHost()).port(DEFAULT_PORT).build();
}
