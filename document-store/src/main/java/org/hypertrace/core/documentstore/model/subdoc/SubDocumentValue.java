package org.hypertrace.core.documentstore.model.subdoc;

import javax.annotation.Nonnull;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.model.Hashable;
import org.hypertrace.core.documentstore.model.Printable;
import org.hypertrace.core.documentstore.model.subdoc.visitor.SubDocumentValueVisitor;

@SuppressWarnings("UnnecessaryInterfaceModifier")
public interface SubDocumentValue extends Hashable, Printable {
  <T> T accept(final SubDocumentValueVisitor visitor);

  public static SubDocumentValue of(@Nonnull final Document document) {
    return new NestedSubDocumentValue(document);
  }

  public static SubDocumentValue of(@Nonnull final Number value) {
    return new PrimitiveSubDocumentValue(value);
  }

  public static SubDocumentValue of(@Nonnull final String value) {
    return new PrimitiveSubDocumentValue(value);
  }

  public static SubDocumentValue of(@Nonnull final Boolean value) {
    return new PrimitiveSubDocumentValue(value);
  }

  public static SubDocumentValue of(@Nonnull final Document[] documents) {
    return new MultiValuedNestedSubDocumentValue(documents);
  }

  public static SubDocumentValue of(@Nonnull final Number[] values) {
    return new MultiValuedPrimitiveSubDocumentValue(values);
  }

  public static SubDocumentValue of(@Nonnull final String[] values) {
    return new MultiValuedPrimitiveSubDocumentValue(values);
  }

  public static SubDocumentValue of(@Nonnull final Boolean[] values) {
    return new MultiValuedPrimitiveSubDocumentValue(values);
  }
}
