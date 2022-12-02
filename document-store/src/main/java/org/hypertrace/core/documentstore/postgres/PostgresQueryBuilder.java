package org.hypertrace.core.documentstore.postgres;

import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.REMOVE;
import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.UNSET;
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
import org.hypertrace.core.documentstore.model.subdoc.UpdateOperator;
import org.hypertrace.core.documentstore.postgres.Params.Builder;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.subdoc.PostgresSubDocumentArrayGetter;
import org.hypertrace.core.documentstore.postgres.subdoc.PostgresSubDocumentValueParser;
import org.hypertrace.core.documentstore.query.Query;

@RequiredArgsConstructor
public class PostgresQueryBuilder {
  @Getter private final String collectionName;
  private final PostgresSubDocumentArrayGetter subDocArrayGetter =
      new PostgresSubDocumentArrayGetter();

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
      final String newDocument = getNewDocument(baseField, path, update, paramsBuilder);
      selectQuery =
          String.format(
              "(SELECT %s, %s AS %s FROM %s AS t%d)", ID, newDocument, DOCUMENT, selectQuery, i);
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

  private String getNewDocument(
      final String baseField,
      final String[] path,
      final SubDocumentUpdate update,
      final Builder paramBuilder) {
    final SubDocumentValue value = update.getSubDocumentValue();

    if (update.getOperator() == UNSET) {
      paramBuilder.addObjectParam(formatSubDocPath(String.join(".", path)));
      return String.format("%s #- ?::text[]", baseField);
    }

    final PostgresSubDocumentValueParser valueParser =
        new PostgresSubDocumentValueParser(paramBuilder);

    if (path.length == 0) {
      switch (update.getOperator()) {
        case APPEND:
          return String.format("COALESCE(%s, '[]') || %s", baseField, value.accept(valueParser));

        case ADD:
          final Object[] values1 =
              Arrays.stream((Object[]) value.accept(subDocArrayGetter)).distinct().toArray();
          final StringBuilder builder1 =
              new StringBuilder(String.format("COALESCE(%s, '[]')", baseField));

          for (final Object singleVal : values1) {
            paramBuilder.addObjectParam(singleVal);
            paramBuilder.addObjectParam(singleVal);
            builder1.append(
                String.format(
                    " || CASE WHEN %s @> to_jsonb(?) THEN '[]'::jsonb ELSE jsonb_build_array(?) END",
                    baseField));
          }

          return builder1.toString();

        case REMOVE:
          final Object[] values = value.accept(subDocArrayGetter);
          final StringBuilder builder = new StringBuilder();
          builder.append(String.format("COALESCE(%s, '[]')", baseField));

          for (final Object singleValue : values) {
            paramBuilder.addObjectParam(singleValue);
            builder.append(" - ?");
          }

          return builder.toString();
      }
    }

    if (path.length == 1) {
      if (update.getOperator() == UpdateOperator.SET) {
        paramBuilder.addObjectParam(formatSubDocPath(path[0]));
        return String.format(
            "jsonb_set(COALESCE(%s, '{}'), ?::text[], %s)", baseField, value.accept(valueParser));
      }
    }

    final String[] pathExcludingFirst = Arrays.stream(path).skip(1).toArray(String[]::new);
    final String fieldAccess = prepareFieldAccessorExpr(path[0], baseField).toString();

    if (update.getOperator() == REMOVE) {
      paramBuilder.addObjectParam(path[0]);
      paramBuilder.addObjectParam(formatSubDocPath(path[0]));
      final String nestedSetQuery =
          getNewDocument(fieldAccess, pathExcludingFirst, update, paramBuilder);
      return String.format(
          "CASE WHEN %s ?? ? THEN jsonb_set(%s, ?::text[], %s) ELSE %s END",
          baseField, baseField, nestedSetQuery, baseField);
    } else {
      paramBuilder.addObjectParam(formatSubDocPath(path[0]));
      final String nestedSetQuery =
          getNewDocument(fieldAccess, pathExcludingFirst, update, paramBuilder);
      return String.format(
          "jsonb_set(COALESCE(%s, '{}'), ?::text[], %s)", baseField, nestedSetQuery);
    }
  }
}
