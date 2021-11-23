package org.hypertrace.core.documentstore.mongo.parser;

import static org.hypertrace.core.documentstore.mongo.MongoCollection.ID_KEY;

import com.mongodb.BasicDBObject;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.type.GroupingExpression;
import org.hypertrace.core.documentstore.parser.GroupingExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SelectingExpressionVisitor;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SelectionSpec;

public final class MongoGroupingExpressionParser extends MongoExpressionParser
    implements GroupingExpressionVisitor {

  private static final String GROUP_CLAUSE = "$group";

  MongoGroupingExpressionParser(Query query) {
    super(query);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final FunctionExpression expression) {
    // To support this, we need to take an alias for GroupingExpressions
    throw new UnsupportedOperationException(
        String.format(
            "Grouping by a function ($%s) is not yet supported by this library for MongoDB",
            expression));
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final IdentifierExpression expression) {
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

    MongoSelectingExpressionParser baseParser =
        new MongoAggregationSelectingExpressionParser(query);

    Map<String, Object> definition =
        selectionSpecs.stream()
            .map(spec -> MongoGroupingExpressionParser.parse(baseParser, spec))
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

  private static Map<String, Object> parse(
      final MongoSelectingExpressionParser baseParser, final SelectionSpec spec) {
    SelectingExpressionVisitor parser =
        new MongoProjectionSelectingExpressionParser(spec.getAlias(), baseParser);
    return spec.getExpression().visit(parser);
  }

  private Map<String, Object> parse(final GroupingExpression expression) {
    MongoGroupingExpressionParser parser = new MongoGroupingExpressionParser(query);
    return expression.visit(parser);
  }
}
