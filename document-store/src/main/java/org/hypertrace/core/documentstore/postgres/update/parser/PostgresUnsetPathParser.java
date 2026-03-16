package org.hypertrace.core.documentstore.postgres.update.parser;

import static org.hypertrace.core.documentstore.model.subdoc.SubDocument.PATH_SEPARATOR;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.formatSubDocPath;

public class PostgresUnsetPathParser implements PostgresUpdateOperationParser {

  @Override
  public String parseNonJsonbField(final UpdateParserInput input) {
    return String.format("\"%s\" = NULL", input.getBaseField());
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
