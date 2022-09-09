package org.hypertrace.core.documentstore.model.subdoc;

import javax.annotation.Nonnull;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.model.subdoc.visitor.SubDocumentValueVisitor;

public interface SubDocumentValue {

  /** @return The contained sub-document value */
  Object getValue();

  Object accept(final SubDocumentValueVisitor visitor);

  static SubDocumentValue of(@Nonnull final Document document) {
    return new NestedSubDocumentValue(document);
  }

  static SubDocumentValue of(@Nonnull final Number value) {
    return toPrimitiveValue(value);
  }

  static SubDocumentValue of(@Nonnull final String value) {
    return toPrimitiveValue(value);
  }

  static SubDocumentValue of(@Nonnull final Boolean value) {
    return toPrimitiveValue(value);
  }

  static SubDocumentValue of(@Nonnull final Number[] values) {
    return toPrimitiveValue(values);
  }

  static SubDocumentValue of(@Nonnull final String[] values) {
    return toPrimitiveValue(values);
  }

  static SubDocumentValue of(@Nonnull final Boolean[] values) {
    return toPrimitiveValue(values);
  }

  private static <T> SubDocumentValue toPrimitiveValue(@Nonnull final T value) {
    return new PrimitiveSubDocumentValue(value);
  }
}
