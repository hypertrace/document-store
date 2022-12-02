package org.hypertrace.core.documentstore.postgres;

import static java.util.stream.Collectors.joining;
import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.REMOVE_ALL_FROM_LIST;
import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.SET;
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
import java.util.stream.IntStream;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
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
      return getNewDocumentForUnset(baseField, path, paramBuilder);
    }

    final PostgresSubDocumentValueParser valueParser =
        new PostgresSubDocumentValueParser(paramBuilder);

    if (path.length == 0) {
      switch (update.getOperator()) {
        case APPEND_TO_LIST:
          return getNewDocumentForAppendToList(baseField, value, valueParser);

        case ADD_TO_LIST_IF_ABSENT:
          return getNewDocumentForAddToListIfAbsent(baseField, paramBuilder, value);
      }
    }

    final String fieldAccess = prepareFieldAccessorExpr(path[0], baseField).toString();

    if (path.length == 1) {
      if (update.getOperator() == SET) {
        return getNewDocumentForSetLeafValue(baseField, value, valueParser, path[0], paramBuilder);
      } else if (update.getOperator() == REMOVE_ALL_FROM_LIST) {
        return getNewDocumentForRemoveAllFromListLeafValue(
            baseField, path[0], paramBuilder, value, fieldAccess);
      }
    }

    if (update.getOperator() == REMOVE_ALL_FROM_LIST) {
      return getNewDocumentForRemoveAllFromListInternalValue(
          baseField, path, update, paramBuilder, fieldAccess);
    } else {
      return getNewDocumentForSetInternalValue(baseField, path, update, paramBuilder, fieldAccess);
    }
  }

  private String getNewDocumentForSetInternalValue(
      String baseField,
      String[] path,
      SubDocumentUpdate update,
      Builder paramBuilder,
      String fieldAccess) {
    final String[] pathExcludingFirst = Arrays.stream(path).skip(1).toArray(String[]::new);
    paramBuilder.addObjectParam(formatSubDocPath(path[0]));
    final String nestedSetQuery =
        getNewDocument(fieldAccess, pathExcludingFirst, update, paramBuilder);
    return String.format("jsonb_set(COALESCE(%s, '{}'), ?::text[], %s)", baseField, nestedSetQuery);
  }

  private String getNewDocumentForSetLeafValue(
      String baseField,
      SubDocumentValue value,
      PostgresSubDocumentValueParser valueParser,
      String subDocPath,
      Builder paramBuilder) {
    paramBuilder.addObjectParam(formatSubDocPath(subDocPath));
    return String.format(
        "jsonb_set(COALESCE(%s, '{}'), ?::text[], %s)", baseField, value.accept(valueParser));
  }

  private String getNewDocumentForRemoveAllFromListInternalValue(
      String baseField,
      String[] path,
      SubDocumentUpdate update,
      Builder paramBuilder,
      String fieldAccess) {
    final String[] pathExcludingFirst = Arrays.stream(path).skip(1).toArray(String[]::new);
    paramBuilder.addObjectParam(path[0]);
    paramBuilder.addObjectParam(formatSubDocPath(path[0]));
    final String nestedSetQuery =
        getNewDocument(fieldAccess, pathExcludingFirst, update, paramBuilder);
    return String.format(
        "CASE WHEN %s ?? ? THEN jsonb_set(%s, ?::text[], %s) ELSE %s END",
        baseField, baseField, nestedSetQuery, baseField);
  }

  private String getNewDocumentForRemoveAllFromListLeafValue(
      String baseField,
      String subDocPath,
      Builder paramBuilder,
      SubDocumentValue value,
      String fieldAccess) {
    final Object[] values = value.accept(subDocArrayGetter);
    paramBuilder.addObjectParam(formatSubDocPath(subDocPath));
    Arrays.stream(values).forEach(paramBuilder::addObjectParam);

    final String filter =
        IntStream.range(0, values.length).mapToObj(i -> "to_jsonb(?)").collect(joining(", "));
    return String.format(
        "jsonb_set(%s, ?::text[], "
            + "(SELECT jsonb_agg(value) "
            + "FROM jsonb_array_elements(%s) t(value) "
            + "WHERE value NOT IN (%s)))",
        baseField, fieldAccess, filter);
  }

  private String getNewDocumentForAddToListIfAbsent(
      String baseField, Builder paramBuilder, SubDocumentValue value) {
    final Object[] values =
        Arrays.stream((Object[]) value.accept(subDocArrayGetter)).distinct().toArray();
    final StringBuilder builder = new StringBuilder(String.format("COALESCE(%s, '[]')", baseField));

    for (final Object singleValue : values) {
      paramBuilder.addObjectParam(singleValue);
      paramBuilder.addObjectParam(singleValue);
      builder.append(
          String.format(
              " || CASE WHEN %s @> to_jsonb(?) THEN '[]'::jsonb ELSE jsonb_build_array(?) END",
              baseField));
    }

    return builder.toString();
  }

  private String getNewDocumentForAppendToList(
      String baseField, SubDocumentValue value, PostgresSubDocumentValueParser valueParser) {
    return String.format("COALESCE(%s, '[]') || %s", baseField, value.accept(valueParser));
  }

  private String getNewDocumentForUnset(String baseField, String[] path, Builder paramBuilder) {
    paramBuilder.addObjectParam(formatSubDocPath(String.join(".", path)));
    return String.format("%s #- ?::text[]", baseField);
  }
}
