package org.hypertrace.core.documentstore.postgres.update.parser;

import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.postgres.subdoc.PostgresSubDocumentArrayGetter;
import org.hypertrace.core.documentstore.postgres.subdoc.PostgresSubDocumentValueAddToListIfAbsentParser;

public class PostgresAddToListIfAbsentParser implements PostgresUpdateOperationParser {

  @Override
  public String parseNonJsonbField(final UpdateParserInput input) {
    final SubDocumentValue value = input.getUpdate().getSubDocumentValue();

    // Extract array values directly for top-level array columns
    final PostgresSubDocumentArrayGetter arrayGetter = new PostgresSubDocumentArrayGetter();
    Object[] arrayValues = value.accept(arrayGetter).values();
    input.getParamsBuilder().addObjectParam(arrayValues);

    // For top-level array columns: add unique values using ARRAY(SELECT DISTINCT unnest(...))
    String arrayType =
        input.getColumnType() != null ? input.getColumnType().getArraySqlType() : "text[]";
    return String.format(
        "\"%s\" = ARRAY(SELECT DISTINCT unnest(COALESCE(\"%s\", '{}') || ?::%s))",
        input.getBaseField(), input.getBaseField(), arrayType);
  }

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
