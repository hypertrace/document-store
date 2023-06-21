package org.hypertrace.core.documentstore.model.config;

import java.util.Arrays;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.experimental.FieldDefaults;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public enum DatabaseType {
  MONGO("mongo"),
  POSTGRES("postgres"),
  ;

  String type;

  static DatabaseType getType(final String type) {
    return Arrays.stream(DatabaseType.values())
        .filter(databaseType -> type.equalsIgnoreCase(databaseType.type))
        .findFirst()
        .orElseThrow();
  }
}
