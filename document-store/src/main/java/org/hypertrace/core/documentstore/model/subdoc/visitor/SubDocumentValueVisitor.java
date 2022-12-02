package org.hypertrace.core.documentstore.model.subdoc.visitor;

import org.hypertrace.core.documentstore.model.subdoc.MultiValuedNestedSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.MultiValuedPrimitiveSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.NestedSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.NullSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.PrimitiveSubDocumentValue;

public interface SubDocumentValueVisitor {
  <T> T visit(final PrimitiveSubDocumentValue value);

  <T> T visit(final MultiValuedPrimitiveSubDocumentValue value);

  <T> T visit(final NestedSubDocumentValue value);

  <T> T visit(final MultiValuedNestedSubDocumentValue value);

  <T> T visit(final NullSubDocumentValue value);
}
