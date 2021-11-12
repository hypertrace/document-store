package org.hypertrace.core.documentstore.mongo.parser;

import static java.util.function.Predicate.not;
import static org.hypertrace.core.documentstore.mongo.MongoCollection.ID_KEY;

import com.mongodb.BasicDBObject;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.type.GroupingExpression;
import org.hypertrace.core.documentstore.parser.GroupingExpressionParser;
import org.hypertrace.core.documentstore.query.Selection;
import org.hypertrace.core.documentstore.query.WhitelistedSelection;

public class MongoGroupingExpressionParser implements GroupingExpressionParser {

  private static final String GROUP_CLAUSE = "$group";

  @Override
  public Map<String, Object> parse(final FunctionExpression expression) {
    throw new UnsupportedOperationException(
        String.format("Cannot group a function ($%s) in MongoDB", expression));
  }

  @Override
  public Map<String, Object> parse(final IdentifierExpression expression) {
    String identifier = MongoIdentifierExpressionParser.parse(expression);
    return Map.of(identifier, "$" + identifier);
  }

  public static BasicDBObject getGroupClause(
      final List<Selection> selections, final List<GroupingExpression> expressions) {
    Map<String, Object> groupExp;

    if (expressions == null) {
      groupExp =
          new HashMap<>() {
            {
              put(ID_KEY, null);
            }
          };
    } else {
      Map<String, Object> groups =
          expressions.stream()
              .map(MongoGroupingExpressionParser::parse)
              .reduce(
                  new LinkedHashMap<>(),
                  (first, second) -> {
                    first.putAll(second);
                    return first;
                  });

      groupExp = Map.of(ID_KEY, groups);
    }

    Map<String, Object> definition =
        selections.stream()
            .filter(not(Selection::allColumnsSelected))
            .map(sel -> (WhitelistedSelection) sel)
            .map(MongoGroupingExpressionParser::parse)
            .reduce(
                new LinkedHashMap<>(),
                (first, second) -> {
                  first.putAll(second);
                  return first;
                });

    definition.putAll(groupExp);
    return new BasicDBObject(GROUP_CLAUSE, definition);
  }

  private static Map<String, Object> parse(final WhitelistedSelection selection) {
    if (!selection.isAggregation()) {
      return Map.of();
    }

    AggregateExpression expression = (AggregateExpression) selection.getExpression();
    Object parsed = MongoAggregateExpressionParser.parse(expression);
    return Map.of(selection.getAlias(), parsed);
  }

  private static Map<String, Object> parse(final GroupingExpression expression) {
    MongoGroupingExpressionParser parser = new MongoGroupingExpressionParser();
    return (Map<String, Object>) expression.parse(parser);
  }
}
