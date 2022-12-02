package org.hypertrace.core.documentstore.postgres.subdoc;

import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.prepareParameterizedStringForList;

import java.util.Arrays;
import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.model.subdoc.MultiValuedNestedSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.MultiValuedPrimitiveSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.NestedSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.NullSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.PrimitiveSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.visitor.SubDocumentValueVisitor;
import org.hypertrace.core.documentstore.postgres.Params;

@AllArgsConstructor
public class PostgresSubDocumentValueParser implements SubDocumentValueVisitor {
  private final Params.Builder paramsBuilder;

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final PrimitiveSubDocumentValue value) {
    paramsBuilder.addObjectParam(value.getValue());
    return "to_jsonb(?)";
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final MultiValuedPrimitiveSubDocumentValue value) {
    final List<Object> values = Arrays.asList(value.getValues());
    return "jsonb_build_array" + prepareParameterizedStringForList(values, paramsBuilder);
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final NestedSubDocumentValue value) {
    paramsBuilder.addObjectParam(value.getJsonValue());
    return "?::jsonb";
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final MultiValuedNestedSubDocumentValue value) {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final NullSubDocumentValue value) {
    throw new UnsupportedOperationException();
  }
}
