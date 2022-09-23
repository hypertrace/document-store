package org.hypertrace.core.documentstore.model.subdoc;

import static lombok.AccessLevel.PACKAGE;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.hypertrace.core.documentstore.model.subdoc.visitor.SubDocumentValueVisitor;

@AllArgsConstructor(access = PACKAGE)
@Getter
public class PrimitiveSubDocumentValue implements SubDocumentValue {
  private final Object value;

  @Override
  public <T> T accept(final SubDocumentValueVisitor visitor) {
    return visitor.visit(this);
  }
}
