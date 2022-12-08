package org.hypertrace.core.documentstore.postgres.subdoc;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.prepareParameterizedStringForJsonList;

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
public class PostgresSubDocumentValueAddToListIfAbsentParser
    implements SubDocumentValueVisitor<String> {
  private final Params.Builder paramsBuilder;
  private final String baseField;

  @Override
  public String visit(final PrimitiveSubDocumentValue value) {
    return baseString() + parsePrimitive(value.getValue());
  }

  @Override
  public String visit(final MultiValuedPrimitiveSubDocumentValue value) {
    return baseString()
        + Arrays.stream(value.getValues()).distinct().map(this::parsePrimitive).collect(joining());
  }

  @Override
  public String visit(final NestedSubDocumentValue value) {
    return baseString() + parseNested(new Object[] {value.getJsonValue()});
  }

  @Override
  public String visit(final MultiValuedNestedSubDocumentValue value) {
    return baseString() + parseNested(value.getJsonValues());
  }

  @Override
  public String visit(final NullSubDocumentValue value) {
    throw new UnsupportedOperationException();
  }

  private String baseString() {
    return String.format("COALESCE(%s, '[]')", baseField);
  }

  private String parsePrimitive(final Object value) {
    // If the element exists in the array, concatenate with empty array,
    // otherwise concatenate with the singleton array containing that element
    paramsBuilder.addObjectParam(value);
    paramsBuilder.addObjectParam(value);
    return String.format(
        " || CASE WHEN %s @> to_jsonb(?) THEN '[]'::jsonb ELSE jsonb_build_array(?) END",
        baseField);
  }

  private String parseNested(final Object[] values) {
    // Compute the set difference and concatenate
    final List<Object> distinctValues =
        Arrays.stream(values).distinct().collect(toUnmodifiableList());
    return String.format(
        " || (SELECT jsonb_agg(value) FROM jsonb_array_elements(jsonb_build_array %s) WHERE value NOT IN (SELECT jsonb_array_elements(%s)))",
        prepareParameterizedStringForJsonList(distinctValues, paramsBuilder), baseField);
  }
}
