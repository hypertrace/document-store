package org.hypertrace.core.documentstore.postgres.subdoc;

import org.hypertrace.core.documentstore.model.subdoc.NestedSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.PrimitiveSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.visitor.SubDocumentValueVisitor;

public class PostgresSubDocumentValueGetter implements SubDocumentValueVisitor {

  @SuppressWarnings("unchecked")
  @Override
  public Object visit(final PrimitiveSubDocumentValue value) {
    return value.getValue();
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final NestedSubDocumentValue value) {
    return value.getJsonValue();
  }
}
