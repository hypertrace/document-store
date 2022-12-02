package org.hypertrace.core.documentstore.postgres.subdoc;

import org.hypertrace.core.documentstore.model.subdoc.MultiValuedNestedSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.MultiValuedPrimitiveSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.NestedSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.NullSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.PrimitiveSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.visitor.SubDocumentValueVisitor;

public class PostgresSubDocumentArrayGetter implements SubDocumentValueVisitor {

  @Override
  public Object[] visit(final PrimitiveSubDocumentValue value) {
    return new Object[] {value.getValue()};
  }

  @Override
  public Object[] visit(final MultiValuedPrimitiveSubDocumentValue value) {
    return value.getValues();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Void visit(final NestedSubDocumentValue value) {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Void visit(final MultiValuedNestedSubDocumentValue value) {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Void visit(final NullSubDocumentValue value) {
    throw new UnsupportedOperationException();
  }
}
