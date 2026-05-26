package org.hypertrace.core.documentstore.model.options;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;
import org.hypertrace.core.documentstore.ReturnOptions;

@Value
@Builder
public class WriteAndReturnOptions {

  public static final WriteAndReturnOptions DEFAULT_OPTIONS =
      WriteAndReturnOptions.builder().build();

  /** Which document image(s) to return. Defaults to {@link ReturnOptions#NONE}. */
  @Default ReturnOptions returnOptions = ReturnOptions.NONE;

  /**
   * If {@code true} (default), the write and the BEFORE/AFTER capture occur in a single round-trip
   * such that the returned BEFORE/AFTER images reflect exactly the row(s) this call wrote.
   *
   * <p>If {@code false}, implementations MAY use separate SELECT and INSERT statements. In that
   * case, under concurrent writers, the returned BEFORE image may not correspond to the row
   * actually replaced by this call, and the returned AFTER image may reflect a later writer's
   * value. Per-row consistency between BEFORE/AFTER and what this call wrote is not guaranteed.
   */
  @Default boolean atomic = true;
}
