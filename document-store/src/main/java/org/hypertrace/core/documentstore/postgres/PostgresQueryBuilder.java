package org.hypertrace.core.documentstore.postgres;

import static org.hypertrace.core.documentstore.postgres.PostgresCollection.DOCUMENT;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.DOC_PATH_SEPARATOR;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.ID;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.formatSubDocPath;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.prepareFieldAccessorExpr;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Stack;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.documentstore.postgres.Params.Builder;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.subdoc.PostgresSubDocumentValueGetter;
import org.hypertrace.core.documentstore.postgres.subdoc.PostgresSubDocumentValueParser;
import org.hypertrace.core.documentstore.query.Query;

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
      final Query query,
      final Collection<SubDocumentUpdate> updates,
      final Params.Builder paramBuilder) {
    final PostgresQueryParser baseQueryParser = new PostgresQueryParser(collectionName, query);
    String selectQuery =
        String.format(
            "(SELECT %s, %s FROM %s AS t0 %s)",
            ID, DOCUMENT, collectionName, baseQueryParser.buildFilterClause());

    final Stack<Params.Builder> paramsStack = new Stack<>();
    paramsStack.push(baseQueryParser.getParamsBuilder());

    final Iterator<SubDocumentUpdate> updateIterator = updates.iterator();

    for (int i = 1; updateIterator.hasNext(); i++) {
      final SubDocumentUpdate update = updateIterator.next();
      final String[] path = update.getSubDocument().getPath().split(DOC_PATH_SEPARATOR);
      final Params.Builder paramsBuilder = Params.newBuilder();
      paramsStack.push(paramsBuilder);

      final String baseField = String.format("t%d.%s", i, DOCUMENT);
      final String setCommand =
          getJsonbSetCall(baseField, path, update.getSubDocumentValue(), paramsBuilder);
      selectQuery =
          String.format(
              "(SELECT %s, %s AS %s FROM %s AS t%d)", ID, setCommand, DOCUMENT, selectQuery, i);
    }

    // Since the sub-query is present in the FROM-clause, the parameters should be processed in the
    // LIFO order
    while (!paramsStack.empty()) {
      for (final Object value : paramsStack.pop().build().getObjectParams().values()) {
        paramBuilder.addObjectParam(value);
      }
    }

    return String.format(
        "WITH concatenated AS %s "
            + "UPDATE %s "
            + "SET %s=concatenated.%s "
            + "FROM concatenated "
            + "WHERE %s.%s=concatenated.%s",
        selectQuery, collectionName, DOCUMENT, DOCUMENT, collectionName, ID, ID);
  }

  private String getJsonbSetCall(
      final String baseField,
      final String[] path,
      final SubDocumentValue value,
      final Builder paramBuilder) {
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
