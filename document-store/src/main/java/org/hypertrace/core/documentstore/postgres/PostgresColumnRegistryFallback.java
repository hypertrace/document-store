package org.hypertrace.core.documentstore.postgres;

import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;

/**
 * Fallback implementation of PostgresColumnRegistry that maintains original hardcoded behavior when
 * database metadata queries fail.
 *
 * <p>This ensures system continues to function even if registry creation encounters errors, falling
 * back to the original OUTER_COLUMNS approach.
 */
@Slf4j
public class PostgresColumnRegistryFallback implements PostgresColumnRegistry {

  public PostgresColumnRegistryFallback() {
    log.debug("Created PostgresColumnRegistryFallback using hardcoded OUTER_COLUMNS");
  }

  @Override
  public boolean isFirstClassColumn(String fieldName) {
    // Fall back to original hardcoded behavior
    return fieldName != null && PostgresUtils.OUTER_COLUMNS.contains(fieldName);
  }

  @Override
  public Optional<PostgresDataType> getColumnDataType(String fieldName) {
    // Map the hardcoded columns to their known types
    if (fieldName == null) {
      return Optional.empty();
    }

    switch (fieldName) {
      case "id":
        return Optional.of(PostgresDataType.TEXT);
      case "created_at":
      case "updated_at":
        // These are timestamp types, but we'll map them as first-class for compatibility
        return Optional.of(PostgresDataType.TEXT); // Can be enhanced to support timestamp later
      default:
        return Optional.empty();
    }
  }

  @Override
  public Set<String> getAllFirstClassColumns() {
    return Set.copyOf(PostgresUtils.OUTER_COLUMNS);
  }

  @Override
  public boolean supportsDataType(PostgresDataType dataType) {
    return dataType != null && dataType.isFirstClassType();
  }
}
