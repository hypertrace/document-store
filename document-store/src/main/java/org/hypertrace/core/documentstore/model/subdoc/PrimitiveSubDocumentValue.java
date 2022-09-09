package org.hypertrace.core.documentstore.model.subdoc;

import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.model.subdoc.visitor.SubDocumentValueVisitor;

@AllArgsConstructor
public class PrimitiveSubDocumentValue implements SubDocumentValue {
  private final Object value;

  @Override
  public Object getValue() {
    return value;
  }

  @Override
  public Object accept(final SubDocumentValueVisitor visitor) {
    return visitor.visit(this);
  }
}
