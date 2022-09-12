package org.hypertrace.core.documentstore.model.subdoc;

import javax.annotation.Nonnull;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.model.subdoc.visitor.SubDocumentValueVisitor;

public interface SubDocumentValue {
  <T> T accept(final SubDocumentValueVisitor visitor);

  static SubDocumentValue of(@Nonnull final Document document) {
    return new NestedSubDocumentValue(document);
  }

  static SubDocumentValue of(@Nonnull final Number value) {
    return new PrimitiveSubDocumentValue(value);
  }

  static SubDocumentValue of(@Nonnull final String value) {
    return new PrimitiveSubDocumentValue(value);
  }

  static SubDocumentValue of(@Nonnull final Boolean value) {
    return new PrimitiveSubDocumentValue(value);
  }

  static SubDocumentValue of(@Nonnull final Number[] values) {
    return new PrimitiveSubDocumentValue(values);
  }

  static SubDocumentValue of(@Nonnull final String[] values) {
    return new PrimitiveSubDocumentValue(values);
  }

  static SubDocumentValue of(@Nonnull final Boolean[] values) {
    return new PrimitiveSubDocumentValue(values);
  }
}
