package org.hypertrace.core.documentstore.model.subdoc;

import static java.util.stream.Collectors.joining;
import static lombok.AccessLevel.PACKAGE;

import java.util.Arrays;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.model.subdoc.visitor.SubDocumentValueVisitor;

@EqualsAndHashCode
@AllArgsConstructor(access = PACKAGE)
public class MultiValuedNestedSubDocumentValue implements SubDocumentValue {
  private final Document[] document;

  public String[] getJsonValues() {
    return Arrays.stream(document).map(Document::toJson).toArray(String[]::new);
  }

  @Override
  public <T> T accept(final SubDocumentValueVisitor<T> visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return "[" + Arrays.stream(document).map(Document::toJson).collect(joining(", ")) + "]";
  }
}
