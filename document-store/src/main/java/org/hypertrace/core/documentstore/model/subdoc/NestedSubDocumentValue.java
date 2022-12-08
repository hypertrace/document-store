package org.hypertrace.core.documentstore.model.subdoc;

import static lombok.AccessLevel.PACKAGE;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.model.subdoc.visitor.SubDocumentValueVisitor;

@EqualsAndHashCode
@AllArgsConstructor(access = PACKAGE)
public class NestedSubDocumentValue implements SubDocumentValue {
  private final Document document;

  public String getJsonValue() {
    return document.toJson();
  }

  @Override
  public <T> T accept(final SubDocumentValueVisitor<T> visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return document.toJson();
  }
}
