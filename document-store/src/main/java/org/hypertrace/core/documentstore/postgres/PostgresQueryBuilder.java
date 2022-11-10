package org.hypertrace.core.documentstore.postgres;

import static org.hypertrace.core.documentstore.postgres.PostgresCollection.DOCUMENT;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.DOC_PATH_SEPARATOR;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.ID;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.formatSubDocPath;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.prepareFieldAccessorExpr;

import java.util.Arrays;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.subdoc.PostgresSubDocumentValueGetter;
import org.hypertrace.core.documentstore.postgres.subdoc.PostgresSubDocumentValueParser;

@RequiredArgsConstructor
public class PostgresQueryBuilder {
  @Getter private final String collectionName;
  private final PostgresSubDocumentValueParser subDocValueParser =
      new PostgresSubDocumentValueParser();
  private final PostgresSubDocumentValueGetter subDocValueGetter =
      new PostgresSubDocumentValueGetter();

  public String getSubDocUpdateQuery(
      final SubDocumentUpdate update, final String id, final Params.Builder paramBuilder) {
    final String[] path = update.getSubDocument().getPath().split(DOC_PATH_SEPARATOR);
    final String setCommand =
        getJsonbSetCall(DOCUMENT, path, update.getSubDocumentValue(), paramBuilder);
    paramBuilder.addObjectParam(id);

    return String.format(
        "UPDATE %s SET %s=%s WHERE %s=?", collectionName, DOCUMENT, setCommand, ID);
  }

  public String getSubDocUpdateQuery(
      final SubDocumentUpdate update, final PostgresQueryParser parser) {
    final String[] path = update.getSubDocument().getPath().split(DOC_PATH_SEPARATOR);
    final String setCommand =
        getJsonbSetCall(DOCUMENT, path, update.getSubDocumentValue(), parser.getParamsBuilder());
    final String optionalFilter = parser.buildFilter();

    return String.format(
        "UPDATE %s SET %s=%s %s", collectionName, DOCUMENT, setCommand, optionalFilter);
  }

  private String getJsonbSetCall(
      final String baseField,
      final String[] path,
      final SubDocumentValue value,
      final Params.Builder paramBuilder) {
    if (path.length == 1) {
      paramBuilder.addObjectParam(formatSubDocPath(path[0]));
      paramBuilder.addObjectParam(value.accept(subDocValueGetter));
      return String.format(
          "jsonb_set(COALESCE(%s, '{}'), ?::text[], %s)",
          baseField, value.accept(subDocValueParser));
    }

    final String[] pathExcludingFirst = Arrays.stream(path).skip(1).toArray(String[]::new);
    paramBuilder.addObjectParam(formatSubDocPath(path[0]));
    final String fieldAccess = prepareFieldAccessorExpr(path[0], baseField).toString();
    final String nestedSetQuery =
        getJsonbSetCall(fieldAccess, pathExcludingFirst, value, paramBuilder);
    return String.format("jsonb_set(COALESCE(%s, '{}'), ?::text[], %s)", baseField, nestedSetQuery);
  }
}
