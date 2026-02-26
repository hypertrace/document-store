package org.hypertrace.core.documentstore.postgres.update.parser;

import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.documentstore.postgres.subdoc.PostgresSubDocumentArrayGetter;
import org.hypertrace.core.documentstore.postgres.subdoc.PostgresSubDocumentValueParser;

public class PostgresAppendToListParser implements PostgresUpdateOperationParser {

  @Override
  public String parseNonJsonbField(final UpdateParserInput input) {
    final SubDocumentValue value = input.getUpdate().getSubDocumentValue();

    // Extract array values directly for top-level array columns
    final PostgresSubDocumentArrayGetter arrayGetter = new PostgresSubDocumentArrayGetter();
    Object[] arrayValues = value.accept(arrayGetter).values();
    input.getParamsBuilder().addObjectParam(arrayValues);

    // For top-level array columns: "column" = COALESCE("column", '{}') || ?::arrayType
    String arrayType =
        input.getColumnType() != null ? input.getColumnType().getArraySqlType() : "text[]";
    return String.format(
        "\"%s\" = COALESCE(\"%s\", '{}') || ?::%s",
        input.getBaseField(), input.getBaseField(), arrayType);
  }

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
