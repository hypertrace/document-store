package org.hypertrace.core.documentstore.postgres.subdoc;

import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.prepareParameterizedStringForJsonList;
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
public class PostgresSubDocumentValueParser implements SubDocumentValueVisitor<String> {
  private final Params.Builder paramsBuilder;

  @Override
  public String visit(final PrimitiveSubDocumentValue value) {
    paramsBuilder.addObjectParam(value.getValue());
    return "to_jsonb(?)";
  }

  @Override
  public String visit(final MultiValuedPrimitiveSubDocumentValue value) {
    final List<Object> values = Arrays.asList(value.getValues());
    return "jsonb_build_array" + prepareParameterizedStringForList(values, paramsBuilder);
  }

  @Override
  public String visit(final NestedSubDocumentValue value) {
    paramsBuilder.addObjectParam(value.getJsonValue());
    return "?::jsonb";
  }

  @Override
  public String visit(final MultiValuedNestedSubDocumentValue value) {
    final List<Object> values = Arrays.asList(value.getJsonValues());
    return "jsonb_build_array" + prepareParameterizedStringForJsonList(values, paramsBuilder);
  }

  @Override
  public String visit(final NullSubDocumentValue value) {
    throw new UnsupportedOperationException();
  }
}
