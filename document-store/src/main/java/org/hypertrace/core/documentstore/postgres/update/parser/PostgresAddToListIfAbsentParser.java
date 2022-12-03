package org.hypertrace.core.documentstore.postgres.update.parser;

import java.util.Arrays;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.postgres.subdoc.PostgresSubDocumentArrayGetter;

public class PostgresAddToListIfAbsentParser implements PostgresUpdateOperationParser {

  @Override
  public String parseInternal(final UpdateParserInput input) {
    return new PostgresSetValueParser(this, 0).parseInternal(input);
  }

  @Override
  public String parseLeaf(final UpdateParserInput input) {
    final String baseField = input.getBaseField();
    final SubDocumentValue value = input.getUpdate().getSubDocumentValue();
    final Params.Builder paramsBuilder = input.getParamsBuilder();
    final PostgresSubDocumentArrayGetter subDocArrayGetter = new PostgresSubDocumentArrayGetter();

    final Object[] values = Arrays.stream(value.accept(subDocArrayGetter)).distinct().toArray();
    final StringBuilder builder = new StringBuilder(String.format("COALESCE(%s, '[]')", baseField));

    for (final Object singleValue : values) {
      paramsBuilder.addObjectParam(singleValue);
      paramsBuilder.addObjectParam(singleValue);
      builder.append(
          String.format(
              " || CASE WHEN %s @> to_jsonb(?) THEN '[]'::jsonb ELSE jsonb_build_array(?) END",
              baseField));
    }

    return builder.toString();
  }
}
