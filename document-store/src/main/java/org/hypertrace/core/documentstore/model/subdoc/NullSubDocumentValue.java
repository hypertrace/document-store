package org.hypertrace.core.documentstore.model.subdoc;

import static lombok.AccessLevel.PACKAGE;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import org.hypertrace.core.documentstore.model.subdoc.visitor.SubDocumentValueVisitor;

@EqualsAndHashCode
@AllArgsConstructor(access = PACKAGE)
public class NullSubDocumentValue implements SubDocumentValue {
  @Override
  public <T> T accept(final SubDocumentValueVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return "<null-value>";
  }
}
