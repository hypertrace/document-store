package org.hypertrace.core.documentstore;

import javax.annotation.Nullable;
import lombok.Value;

@Value
public class WriteAndReturnResult {

  /** The input key this result corresponds to. Never {@code null}. */
  Key key;

  /**
   * The pre-image of the row. Non-null iff BEFORE was requested AND a row existed for {@link
   * #getKey()} prior to this call; {@code null} otherwise.
   */
  @Nullable Document before;

  /**
   * The post-image of the row. Non-null iff AFTER was requested (always present in that case);
   * {@code null} otherwise.
   */
  @Nullable Document after;
}
