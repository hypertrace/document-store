package org.hypertrace.core.documentstore.postgres.update.parser;

import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.formatSubDocPath;

import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.postgres.subdoc.PostgresSubDocumentValueParser;

public class PostgresAddValueParser implements PostgresUpdateOperationParser {
  @Override
  public String parseInternal(UpdateParserInput input) {
    return new PostgresSetValueParser(this, 1).parseInternal(input);
  }

  @Override
  public String parseLeaf(UpdateParserInput input) {
    final Params.Builder paramsBuilder = input.getParamsBuilder();
    final PostgresSubDocumentValueParser valueParser =
        new PostgresSubDocumentValueParser(paramsBuilder);

    paramsBuilder.addObjectParam(formatSubDocPath(input.getPath()[0]));
    final String parsedValue = input.getUpdate().getSubDocumentValue().accept(valueParser);
    return String.format(
        "jsonb_set(%s, ?::text[], (COALESCE(%s->>'%s', '0')::float + %s::float)::text::jsonb)",
        input.getBaseField(), input.getBaseField(), input.getPath()[0], parsedValue);
  }
}
