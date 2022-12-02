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
public class PrimitiveSubDocumentValue implements SubDocumentValue {
  private final Object value;

  @Override
  public <T> T accept(final SubDocumentValueVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
