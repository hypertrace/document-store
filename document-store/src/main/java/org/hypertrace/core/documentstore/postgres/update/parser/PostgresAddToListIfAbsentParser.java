package org.hypertrace.core.documentstore.postgres.update.parser;

import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.postgres.subdoc.PostgresSubDocumentValueAddToListIfAbsentParser;

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

    return subDocValue.accept(
        new PostgresSubDocumentValueAddToListIfAbsentParser(paramsBuilder, baseField));
  }
}
