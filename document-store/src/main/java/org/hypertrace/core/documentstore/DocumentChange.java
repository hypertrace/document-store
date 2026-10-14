package org.hypertrace.core.documentstore;

import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * The document images returned by a single atomic write. Which images are present depends on the
 * requested {@link org.hypertrace.core.documentstore.model.options.UpsertOptions.ReturnDocuments}.
 */
public final class DocumentChange {

  @Nullable private final Document before;
  @Nullable private final Document after;

  private DocumentChange(@Nullable final Document before, @Nullable final Document after) {
    this.before = before;
    this.after = after;
  }

  /**
   * @param before the document before the write; null if not requested or if the write created the
   *     document
   * @param after the document after the write; null if not requested
   */
  public static DocumentChange of(@Nullable final Document before, @Nullable final Document after) {
    return new DocumentChange(before, after);
  }

  /** Empty when the before-image was not requested, or when the write created the document. */
  public Optional<Document> getBefore() {
    return Optional.ofNullable(before);
  }

  /** Empty when the after-image was not requested. */
  public Optional<Document> getAfter() {
    return Optional.ofNullable(after);
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
    return Objects.equals(before, that.before) && Objects.equals(after, that.after);
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
