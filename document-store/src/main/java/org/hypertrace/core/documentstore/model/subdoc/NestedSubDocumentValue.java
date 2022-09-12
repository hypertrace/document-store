package org.hypertrace.core.documentstore.model.subdoc;

import static lombok.AccessLevel.PACKAGE;

import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.model.subdoc.visitor.SubDocumentValueVisitor;

@AllArgsConstructor(access = PACKAGE)
public class NestedSubDocumentValue implements SubDocumentValue {
  private final Document document;

  public String getJsonValue() {
    return document.toJson();
  }

  @Override
  public <T> T accept(final SubDocumentValueVisitor visitor) {
    return visitor.visit(this);
  }
}
