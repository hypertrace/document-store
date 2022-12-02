package org.hypertrace.core.documentstore.model.subdoc;

import static lombok.AccessLevel.PACKAGE;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.hypertrace.core.documentstore.model.subdoc.visitor.SubDocumentValueVisitor;

@EqualsAndHashCode
@AllArgsConstructor(access = PACKAGE)
@Getter
@ToString
public class MultiValuedPrimitiveSubDocumentValue implements SubDocumentValue {
  private final Object[] values;

  @Override
  public <T> T accept(final SubDocumentValueVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
