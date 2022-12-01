package org.hypertrace.core.documentstore.model.subdoc;

import static java.util.stream.Collectors.toUnmodifiableList;
import static lombok.AccessLevel.PACKAGE;

import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.model.subdoc.visitor.SubDocumentValueVisitor;

@EqualsAndHashCode
@AllArgsConstructor(access = PACKAGE)
public class ArraySubDocumentValue implements SubDocumentValue {
  private final List<Document> documents;

  public List<String> getJsonValues() {
    return documents.stream().map(Document::toJson).collect(toUnmodifiableList());
  }

  @Override
  public <T> T accept(final SubDocumentValueVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return "[" + documents.stream().map(Document::toJson).collect(Collectors.joining(", ")) + "]";
  }
}
