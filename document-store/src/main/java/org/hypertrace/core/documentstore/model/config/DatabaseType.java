package org.hypertrace.core.documentstore.model.config;

import java.util.Arrays;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.experimental.FieldDefaults;

@Getter
@Accessors(fluent = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public enum DatabaseType {
  MONGO("mongo"),
  POSTGRES("postgres"),
  ;

  String type;

  public static DatabaseType getType(final String type) {
    return Arrays.stream(DatabaseType.values())
        .filter(databaseType -> type.equalsIgnoreCase(databaseType.type))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Invalid database type: " + type));
  }
}
