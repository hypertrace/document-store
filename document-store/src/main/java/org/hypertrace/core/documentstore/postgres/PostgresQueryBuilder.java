package org.hypertrace.core.documentstore.postgres;

import static java.util.Map.entry;
import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.ADD_TO_LIST_IF_ABSENT;
import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.APPEND_TO_LIST;
import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.INCREMENT;
import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.REMOVE_ALL_FROM_LIST;
import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.SET;
import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.UNSET;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.DOCUMENT;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.DOC_PATH_SEPARATOR;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.ID;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Stack;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.model.subdoc.UpdateOperator;
import org.hypertrace.core.documentstore.postgres.Params.Builder;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.update.parser.PostgresAddToListIfAbsentParser;
import org.hypertrace.core.documentstore.postgres.update.parser.PostgresAppendToListParser;
import org.hypertrace.core.documentstore.postgres.update.parser.PostgresIncrementValueParser;
import org.hypertrace.core.documentstore.postgres.update.parser.PostgresRemoveAllFromListParser;
import org.hypertrace.core.documentstore.postgres.update.parser.PostgresSetValueParser;
import org.hypertrace.core.documentstore.postgres.update.parser.PostgresUnsetPathParser;
import org.hypertrace.core.documentstore.postgres.update.parser.PostgresUpdateOperationParser;
import org.hypertrace.core.documentstore.postgres.update.parser.PostgresUpdateOperationParser.UpdateParserInput;
import org.hypertrace.core.documentstore.query.Query;

@RequiredArgsConstructor
public class PostgresQueryBuilder {
  private static final Map<UpdateOperator, PostgresUpdateOperationParser> UPDATE_PARSER_MAP =
      Map.ofEntries(
          entry(SET, new PostgresSetValueParser()),
          entry(UNSET, new PostgresUnsetPathParser()),
          entry(INCREMENT, new PostgresIncrementValueParser()),
          entry(REMOVE_ALL_FROM_LIST, new PostgresRemoveAllFromListParser()),
          entry(ADD_TO_LIST_IF_ABSENT, new PostgresAddToListIfAbsentParser()),
          entry(APPEND_TO_LIST, new PostgresAppendToListParser()));

  @Getter private final String collectionName;

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
      final String newDocument = getNewDocumentQuery(baseField, path, update, paramsBuilder);
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

  private String getNewDocumentQuery(
      final String baseField,
      final String[] path,
      final SubDocumentUpdate update,
      final Builder paramsBuilder) {
    final UpdateParserInput input =
        UpdateParserInput.builder()
            .baseField(baseField)
            .path(path)
            .update(update)
            .paramsBuilder(paramsBuilder)
            .build();
    return UPDATE_PARSER_MAP.get(update.getOperator()).parseInternal(input);
  }
}
