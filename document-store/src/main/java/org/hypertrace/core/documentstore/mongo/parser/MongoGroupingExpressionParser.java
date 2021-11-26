package org.hypertrace.core.documentstore.mongo.parser;

import static org.hypertrace.core.documentstore.mongo.MongoCollection.ID_KEY;

import com.mongodb.BasicDBObject;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.type.GroupingExpression;
import org.hypertrace.core.documentstore.parser.GroupingExpressionParser;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SelectionSpec;

public class MongoGroupingExpressionParser extends MongoExpressionParser
    implements GroupingExpressionParser {

  private static final String GROUP_CLAUSE = "$group";

  protected MongoGroupingExpressionParser(Query query) {
    super(query);
  }

  @Override
  public Map<String, Object> parse(final FunctionExpression expression) {
    throw new UnsupportedOperationException(
        String.format("Cannot group a function ($%s) in MongoDB", expression));
  }

  @Override
  public Map<String, Object> parse(final IdentifierExpression expression) {
    String identifier = new MongoIdentifierExpressionParser(query).parse(expression);
    String key = identifier.replaceAll("\\.", "-");
    return Map.of(key, "$" + identifier);
  }

  public static BasicDBObject getGroupClause(final Query query) {
    final List<SelectionSpec> selectionSpecs = query.getSelections();
    final List<GroupingExpression> expressions = query.getAggregations();

    MongoGroupingExpressionParser parser = new MongoGroupingExpressionParser(query);
    Map<String, Object> groupExp;

    if (CollectionUtils.isEmpty(expressions)) {
      groupExp =
          new HashMap<>() {
            {
              put(ID_KEY, null);
            }
          };
    } else {
      Map<String, Object> groups =
          expressions.stream()
              .map(parser::parse)
              .reduce(
                  new LinkedHashMap<>(),
                  (first, second) -> {
                    first.putAll(second);
                    return first;
                  });

      groupExp = Map.of(ID_KEY, groups);
    }

    Map<String, Object> definition =
        selectionSpecs.stream()
            .map(parser::parse)
            .reduce(
                new LinkedHashMap<>(),
                (first, second) -> {
                  first.putAll(second);
                  return first;
                });

    if (MapUtils.isEmpty(definition) && CollectionUtils.isEmpty(expressions)) {
      return new BasicDBObject();
    }

    definition.putAll(groupExp);
    return new BasicDBObject(GROUP_CLAUSE, definition);
  }

  private Map<String, Object> parse(final SelectionSpec selection) {
    if (!selection.isAggregation()) {
      return Map.of();
    }

    AggregateExpression expression = (AggregateExpression) selection.getExpression();
    Object parsed = new MongoAggregateExpressionParser(query).parse(expression);
    return Map.of(selection.getAlias(), parsed);
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> parse(final GroupingExpression expression) {
    MongoGroupingExpressionParser parser = new MongoGroupingExpressionParser(query);
    return (Map<String, Object>) expression.parse(parser);
  }
}
