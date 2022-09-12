package org.hypertrace.core.documentstore.model.subdoc.visitor;

import org.hypertrace.core.documentstore.model.subdoc.NestedSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.PrimitiveSubDocumentValue;

public interface SubDocumentValueVisitor {
  <T> T visit(final PrimitiveSubDocumentValue value);

  <T> T visit(final NestedSubDocumentValue value);
}
