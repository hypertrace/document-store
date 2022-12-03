package org.hypertrace.core.documentstore.postgres.update.parser;

import static org.hypertrace.core.documentstore.model.subdoc.SubDocument.PATH_SEPARATOR;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.formatSubDocPath;

public class PostgresUnsetPathParser implements PostgresUpdateOperationParser {

  @Override
  public String parseInternal(final UpdateParserInput input) {
    input
        .getParamsBuilder()
        .addObjectParam(formatSubDocPath(String.join(PATH_SEPARATOR, input.getPath())));
    return String.format("%s #- ?::text[]", input.getBaseField());
  }

  @Override
  public String parseLeaf(final UpdateParserInput input) {
    throw new IllegalStateException();
  }
}
