package org.hypertrace.core.documentstore.postgres.subdoc;

import java.util.List;
import org.hypertrace.core.documentstore.model.subdoc.ArraySubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.NestedSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.NullSubDocumentValue;
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

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final NullSubDocumentValue value) {
    return "";
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<String> visit(final ArraySubDocumentValue value) {
    return value.getJsonValues();
  }
}
