package org.hypertrace.core.documentstore.postgres.update.parser;

import static java.util.stream.Collectors.joining;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.formatSubDocPath;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.prepareFieldAccessorExpr;

import java.util.Arrays;
import java.util.stream.IntStream;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.postgres.Params.Builder;
import org.hypertrace.core.documentstore.postgres.subdoc.PostgresSubDocumentArrayGetter;
import org.hypertrace.core.documentstore.postgres.subdoc.PostgresSubDocumentArrayGetter.SubDocumentArray;
import org.hypertrace.core.documentstore.postgres.subdoc.PostgresSubDocumentArrayGetter.Type;

public class PostgresRemoveAllFromListParser implements PostgresUpdateOperationParser {

  @Override
  public String parseNonJsonbField(final UpdateParserInput input) {
    final PostgresSubDocumentArrayGetter subDocArrayGetter = new PostgresSubDocumentArrayGetter();
    final SubDocumentArray array =
        input.getUpdate().getSubDocumentValue().accept(subDocArrayGetter);
    final Object[] values = array.values();

    // Add array as single param (not individual values)
    input.getParamsBuilder().addObjectParam(values);

    // For top-level array columns: remove values using array_agg with filter
    String arrayType =
        input.getColumnType() != null ? input.getColumnType().getArraySqlType() : "text[]";
    return String.format(
        "\"%s\" = (SELECT array_agg(elem) FROM unnest(\"%s\") AS elem WHERE NOT (elem = ANY(?::%s)))",
        input.getBaseField(), input.getBaseField(), arrayType);
  }

  @Override
  public String parseInternal(final UpdateParserInput input) {
    final String baseField = input.getBaseField();
    final String[] path = input.getPath();
    final SubDocumentUpdate update = input.getUpdate();
    final Builder paramsBuilder = input.getParamsBuilder();

    if (path.length == 1) {
      return parseLeaf(input);
    }

    final String[] pathExcludingFirst = Arrays.stream(path).skip(1).toArray(String[]::new);
    final String fieldAccess = prepareFieldAccessorExpr(path[0], baseField).toString();

    paramsBuilder.addObjectParam(path[0]);
    paramsBuilder.addObjectParam(formatSubDocPath(path[0]));

    final UpdateParserInput newInput =
        UpdateParserInput.builder()
            .baseField(fieldAccess)
            .path(pathExcludingFirst)
            .update(update)
            .paramsBuilder(paramsBuilder)
            .build();

    // If the baseField exists, then set the recursively removed value, otherwise set the baseField
    // itself (simulating a no-op)
    final String nestedQuery = parseInternal(newInput);
    // Double question mark (??) is to escape the element existence operator (?) so that it is not
    // considered as a parameter placeholder
    return String.format(
        "CASE WHEN %s ?? ? THEN jsonb_set(%s, ?::text[], %s) ELSE %s END",
        baseField, baseField, nestedQuery, baseField);
  }

  @Override
  public String parseLeaf(final UpdateParserInput input) {
    final String baseField = input.getBaseField();
    final String subDocPath = input.getPath()[0];
    final Builder paramsBuilder = input.getParamsBuilder();

    final PostgresSubDocumentArrayGetter subDocArrayGetter = new PostgresSubDocumentArrayGetter();
    final SubDocumentArray array =
        input.getUpdate().getSubDocumentValue().accept(subDocArrayGetter);
    final Object[] values = array.values();
    final Type type = array.type();
    final String fieldAccess = prepareFieldAccessorExpr(subDocPath, baseField).toString();

    paramsBuilder.addObjectParam(formatSubDocPath(subDocPath));
    Arrays.stream(values).forEach(paramsBuilder::addObjectParam);

    final String filter =
        IntStream.range(0, values.length).mapToObj(i -> type.parsed()).collect(joining(", "));
    return String.format(
        "jsonb_set(%s, ?::text[], "
            + "(SELECT COALESCE (jsonb_agg(value), '[]'::jsonb) "
            + "FROM jsonb_array_elements(%s) t(value) "
            + "WHERE value NOT IN (%s)))",
        baseField, fieldAccess, filter);
  }
}
