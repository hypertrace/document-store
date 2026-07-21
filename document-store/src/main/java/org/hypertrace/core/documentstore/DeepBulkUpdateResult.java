package org.hypertrace.core.documentstore;

import java.util.Collections;
import java.util.Set;
import lombok.Getter;

/**
 * Carries information about failed and successful keys. Can be enhanced in future to carry
 * information like failure reasons, etc.
 */
@Getter
public class DeepBulkUpdateResult extends BulkUpdateResult {

  private final Set<Key> failedKeys;
  private final Set<Key> successfulKeys;

  // all keys attempted can be derived from failedKeys + successfulKeys

  public DeepBulkUpdateResult(
      long successfullyUpdated, Set<Key> failedKeys, Set<Key> successfulKeys) {
    super(successfullyUpdated);
    this.failedKeys =
        failedKeys == null || failedKeys.isEmpty()
            ? Collections.emptySet()
            : Collections.unmodifiableSet(failedKeys);
    this.successfulKeys =
        successfulKeys == null || successfulKeys.isEmpty()
            ? Collections.emptySet()
            : Collections.unmodifiableSet(successfulKeys);
  }

  public boolean hasFailures() {
    return !failedKeys.isEmpty();
  }

  public boolean wasSuccessful() {
    return failedKeys.isEmpty();
  }
}
