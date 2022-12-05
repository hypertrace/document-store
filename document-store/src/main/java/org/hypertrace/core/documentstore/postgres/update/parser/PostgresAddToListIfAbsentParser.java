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
    final SubDocumentValue subDocValue = input.getUpdate().getSubDocumentValue();
    final Params.Builder paramsBuilder = input.getParamsBuilder();
    final PostgresSubDocumentArrayGetter subDocArrayGetter = new PostgresSubDocumentArrayGetter();

    // Deduplicate the input values
    final Object[] values =
        Arrays.stream(subDocValue.accept(subDocArrayGetter)).distinct().toArray();

    // Start with an empty array if the original field does not exist
    final StringBuilder builder = new StringBuilder(String.format("COALESCE(%s, '[]')", baseField));

    for (final Object value : values) {
      // If the value is already present in the array, then concatenate with an empty array,
      // otherwise concatenate with the singleton array containing the value
      builder.append(
          String.format(
              " || CASE WHEN %s @> to_jsonb(?) THEN '[]'::jsonb ELSE jsonb_build_array(?) END",
              baseField));
      paramsBuilder.addObjectParam(value);
      paramsBuilder.addObjectParam(value);
    }

    return builder.toString();
  }
}
