package org.hypertrace.core.documentstore.postgres.subdoc;

import static org.hypertrace.core.documentstore.postgres.subdoc.PostgresSubDocumentArrayGetter.Type.OBJECT;
import static org.hypertrace.core.documentstore.postgres.subdoc.PostgresSubDocumentArrayGetter.Type.PRIMITIVE;

import java.util.Arrays;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.Accessors;
import org.hypertrace.core.documentstore.model.subdoc.MultiValuedNestedSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.MultiValuedPrimitiveSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.NestedSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.NullSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.PrimitiveSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.visitor.SubDocumentValueVisitor;
import org.hypertrace.core.documentstore.postgres.subdoc.PostgresSubDocumentArrayGetter.SubDocumentArray;

public class PostgresSubDocumentArrayGetter implements SubDocumentValueVisitor<SubDocumentArray> {

  @Override
  public SubDocumentArray visit(final PrimitiveSubDocumentValue value) {
    return SubDocumentArray.builder()
        .values(new Object[] {value.getValue()})
        .type(PRIMITIVE)
        .build();
  }

  @Override
  public SubDocumentArray visit(final MultiValuedPrimitiveSubDocumentValue value) {
    return SubDocumentArray.builder().values(value.getValues()).type(PRIMITIVE).build();
  }

  @Override
  public SubDocumentArray visit(final NestedSubDocumentValue value) {
    return SubDocumentArray.builder()
        .values(new Object[] {value.getJsonValue()})
        .type(OBJECT)
        .build();
  }

  @Override
  public SubDocumentArray visit(final MultiValuedNestedSubDocumentValue value) {
    return SubDocumentArray.builder()
        .values(Arrays.stream(value.getJsonValues()).toArray())
        .type(OBJECT)
        .build();
  }

  @Override
  public SubDocumentArray visit(final NullSubDocumentValue value) {
    throw new UnsupportedOperationException();
  }

  @Value
  @Builder
  @Accessors(fluent = true)
  public static class SubDocumentArray {
    Object[] values;
    Type type;
  }

  @AllArgsConstructor
  @Getter
  @Accessors(fluent = true)
  public enum Type {
    PRIMITIVE("to_jsonb(?)"),
    OBJECT("?::jsonb"),
    ;

    private final String parsed;
  }
}
