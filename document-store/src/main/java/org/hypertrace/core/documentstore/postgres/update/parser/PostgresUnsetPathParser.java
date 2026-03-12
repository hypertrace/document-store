package org.hypertrace.core.documentstore.postgres.update.parser;

import static org.hypertrace.core.documentstore.model.subdoc.SubDocument.PATH_SEPARATOR;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.formatSubDocPath;

import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresDataType;

public class PostgresUnsetPathParser implements PostgresUpdateOperationParser {

  @Override
  public String parseNonJsonbField(final UpdateParserInput input) {
    String baseField = input.getBaseField();

    if (input.isArray()) {
      // Array columns → empty array
      return String.format("\"%s\" = '{}'", baseField);
    } else if (input.getColumnType() == PostgresDataType.JSONB) {
      // JSONB columns → empty object
      return String.format("\"%s\" = '{}'::jsonb", baseField);
    } else {
      // Other columns → NULL
      return String.format("\"%s\" = NULL", baseField);
    }
  }

  @Override
  public String parseInternal(final UpdateParserInput input) {
    return parse(input);
  }

  @Override
  public String parseLeaf(final UpdateParserInput input) {
    return parse(input);
  }

  private String parse(final UpdateParserInput input) {
    final String formattedPath = formatSubDocPath(String.join(PATH_SEPARATOR, input.getPath()));
    input.getParamsBuilder().addObjectParam(formattedPath);
    return String.format("%s #- ?::text[]", input.getBaseField());
  }
}
