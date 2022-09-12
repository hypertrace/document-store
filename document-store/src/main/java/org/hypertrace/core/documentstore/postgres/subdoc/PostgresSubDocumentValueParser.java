package org.hypertrace.core.documentstore.postgres.subdoc;

import org.hypertrace.core.documentstore.model.subdoc.NestedSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.PrimitiveSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.visitor.SubDocumentValueVisitor;

public class PostgresSubDocumentValueParser implements SubDocumentValueVisitor {

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final PrimitiveSubDocumentValue value) {
    return "to_jsonb(?)";
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final NestedSubDocumentValue value) {
    return "?::jsonb";
  }
}
