package org.hypertrace.core.documentstore.mongo.parser;

import com.mongodb.BasicDBObject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.type.GroupingExpression;
import org.hypertrace.core.documentstore.mongo.MongoCollection;
import org.hypertrace.core.documentstore.parser.GroupingExpressionParser;
import org.hypertrace.core.documentstore.query.Selection;

public class MongoGroupingExpressionParser implements GroupingExpressionParser {

  private static final String GROUP_CLAUSE = "$group";

  @Override
  public Map<String, Object> parse(final FunctionExpression expression) {
    Object value = MongoFunctionExpressionParser.parse(expression);
    return Map.of(MongoCollection.ID_KEY, value);
  }

  @Override
  public Map<String, Object> parse(final IdentifierExpression expression) {
    String identifier = "$" + new MongoIdentifierExpressionParser().parse(expression);
    return Map.of(MongoCollection.ID_KEY, identifier);
  }

  public static BasicDBObject getGroupClause(
      final List<Selection> selections, final List<GroupingExpression> expressions) {
    Map<String, Object> groupExp;

    if (expressions == null) {
      groupExp = Map.of();
    } else {
      groupExp =
          expressions.stream()
              .map(MongoGroupingExpressionParser::parse)
              .reduce(
                  new LinkedHashMap<>(),
                  (first, second) -> {
                    first.putAll(second);
                    return first;
                  });
    }

    Map<String, Object> definition =
        selections.stream()
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

  private static Map<String, Object> parse(final Selection selection) {
    if (!selection.isAggregation()) {
      return Map.of();
    }

    AggregateExpression expression = (AggregateExpression) selection.getExpression();
    Object parsed = new MongoAggregateExpressionParser().parse(expression);
    return Map.of(selection.getAlias(), parsed);
  }

  private static Map<String, Object> parse(final GroupingExpression expression) {
    MongoGroupingExpressionParser parser = new MongoGroupingExpressionParser();
    return (Map<String, Object>) expression.parse(parser);
  }
}
