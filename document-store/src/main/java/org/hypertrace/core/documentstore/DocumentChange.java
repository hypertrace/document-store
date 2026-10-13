package org.hypertrace.core.documentstore;

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.Optional;

/** Before and after images from a single atomic write. */
public final class DocumentChange {

  private final Document before;
  private final Document after;

  private DocumentChange(final Document before, final Document after) {
    this.before = before;
    this.after = requireNonNull(after, "after");
  }

  public static DocumentChange created(final Document after) {
    return new DocumentChange(null, after);
  }

  public static DocumentChange updated(final Document before, final Document after) {
    requireNonNull(before, "before");
    return new DocumentChange(before, after);
  }

  public Optional<Document> getBefore() {
    return Optional.ofNullable(before);
  }

  public Document getAfter() {
    return after;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DocumentChange that = (DocumentChange) o;
    return Objects.equals(before, that.before) && after.equals(that.after);
  }

  @Override
  public int hashCode() {
    return Objects.hash(before, after);
  }

  @Override
  public String toString() {
    return "DocumentChange{before=" + before + ", after=" + after + '}';
  }
}
