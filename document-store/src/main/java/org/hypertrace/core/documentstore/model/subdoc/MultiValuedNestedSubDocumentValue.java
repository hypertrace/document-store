package org.hypertrace.core.documentstore.model.subdoc;

import static lombok.AccessLevel.PACKAGE;

import java.util.Arrays;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.model.subdoc.visitor.SubDocumentValueVisitor;

@EqualsAndHashCode
@AllArgsConstructor(access = PACKAGE)
public class MultiValuedNestedSubDocumentValue implements SubDocumentValue {
  private final Document[] documents;

  public String[] getJsonValues() {
    return Arrays.stream(documents).map(Document::toJson).toArray(String[]::new);
  }

  @Override
  public <T> T accept(final SubDocumentValueVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return "["
        + Arrays.stream(documents).map(Document::toJson).collect(Collectors.joining(", "))
        + "]";
  }
}
