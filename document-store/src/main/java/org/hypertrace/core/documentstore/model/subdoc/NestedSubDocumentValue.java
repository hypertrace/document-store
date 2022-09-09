package org.hypertrace.core.documentstore.model.subdoc;

import static lombok.AccessLevel.PACKAGE;

import com.mongodb.BasicDBObject;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.model.subdoc.visitor.SubDocumentValueVisitor;

@AllArgsConstructor(access = PACKAGE)
@Getter
public class NestedSubDocumentValue implements SubDocumentValue {
  private final Document document;

  @Override
  public Object getValue() {
    return document.toJson();
  }

  @Override
  public BasicDBObject accept(final SubDocumentValueVisitor visitor) {
    return visitor.visit(this);
  }
}
