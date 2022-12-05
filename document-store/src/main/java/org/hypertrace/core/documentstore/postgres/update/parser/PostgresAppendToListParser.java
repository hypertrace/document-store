package org.hypertrace.core.documentstore.postgres.update.parser;

import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.documentstore.postgres.subdoc.PostgresSubDocumentValueParser;

public class PostgresAppendToListParser implements PostgresUpdateOperationParser {

  @Override
  public String parseInternal(final UpdateParserInput input) {
    return new PostgresSetValueParser(this, 0).parseInternal(input);
  }

  @Override
  public String parseLeaf(final UpdateParserInput input) {
    final String baseField = input.getBaseField();
    final SubDocumentValue value = input.getUpdate().getSubDocumentValue();
    final PostgresSubDocumentValueParser valueParser =
        new PostgresSubDocumentValueParser(input.getParamsBuilder());

    // Concatenate with an empty array if the original field does not exist
    return String.format("COALESCE(%s, '[]') || %s", baseField, value.accept(valueParser));
  }
}
