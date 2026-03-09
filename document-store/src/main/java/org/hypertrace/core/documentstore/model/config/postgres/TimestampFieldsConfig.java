package org.hypertrace.core.documentstore.model.config.postgres;

import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/**
 * Configuration for auto-managed timestamp fields in a Postgres collection. Specifies which columns
 * should be automatically populated with creation and last update timestamps.
 */
@Value
@Builder
public class TimestampFieldsConfig {

  /**
   * Column name for the creation timestamp. This field is set once when a document is created and
   * preserved on updates.
   */
  @Nullable String created;

  /**
   * Column name for the last update timestamp. This field is updated every time a document is
   * created or modified.
   */
  @Nullable String lastUpdated;

  public Optional<String> getCreated() {
    return Optional.ofNullable(created);
  }

  public Optional<String> getLastUpdated() {
    return Optional.ofNullable(lastUpdated);
  }
}
