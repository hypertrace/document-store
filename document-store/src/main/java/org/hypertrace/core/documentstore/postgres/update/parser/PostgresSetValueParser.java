package org.hypertrace.core.documentstore.postgres.update.parser;

import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.formatSubDocPath;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.prepareFieldAccessorExpr;

import java.util.Arrays;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.postgres.Params.Builder;
import org.hypertrace.core.documentstore.postgres.subdoc.PostgresSubDocumentValueParser;

@AllArgsConstructor
public class PostgresSetValueParser implements PostgresUpdateOperationParser {
  private final PostgresUpdateOperationParser leafParser;
  private final int leafNodePathSize;

  public PostgresSetValueParser() {
    leafParser = this;
    leafNodePathSize = 1;
  }

  @Override
  public String parseNonJsonbField(final UpdateParserInput input) {
    final Params.Builder paramsBuilder = input.getParamsBuilder();
    final PostgresSubDocumentValueParser valueParser =
        new PostgresSubDocumentValueParser(paramsBuilder);

    // For top-level columns, just set the value directly: "column" = ?
    input.getUpdate().getSubDocumentValue().accept(valueParser);
    return String.format("\"%s\" = ?", input.getBaseField());
  }

  @Override
  public String parseInternal(final UpdateParserInput input) {
    final String baseField = input.getBaseField();
    final String[] path = input.getPath();
    final SubDocumentUpdate update = input.getUpdate();
    final Params.Builder paramsBuilder = input.getParamsBuilder();

    if (path.length == leafNodePathSize) {
      return leafParser.parseLeaf(input);
    }

    final String[] pathExcludingFirst = Arrays.stream(path).skip(1).toArray(String[]::new);
    paramsBuilder.addObjectParam(formatSubDocPath(path[0]));
    final String fieldAccess = prepareFieldAccessorExpr(path[0], baseField).toString();

    final UpdateParserInput newInput =
        UpdateParserInput.builder()
            .baseField(fieldAccess)
            .path(pathExcludingFirst)
            .update(update)
            .paramsBuilder(paramsBuilder)
            .build();

    // Since the neither the || operator nor the 'create_missing' (4th argument to jsonb_set)
    // performs a recursive concatenation, we build the query recursively
    final String nestedSetQuery = parseInternal(newInput);
    return String.format("jsonb_set(COALESCE(%s, '{}'), ?::text[], %s)", baseField, nestedSetQuery);
  }

  @Override
  public String parseLeaf(final UpdateParserInput input) {
    final Builder paramsBuilder = input.getParamsBuilder();
    final PostgresSubDocumentValueParser valueParser =
        new PostgresSubDocumentValueParser(paramsBuilder);

    paramsBuilder.addObjectParam(formatSubDocPath(input.getPath()[0]));
    final String parsedValue = input.getUpdate().getSubDocumentValue().accept(valueParser);
    return String.format(
        "jsonb_set(COALESCE(%s, '{}'), ?::text[], %s)", input.getBaseField(), parsedValue);
  }
}
