package org.hypertrace.core.documentstore.mongo.parser;

import static org.hypertrace.core.documentstore.mongo.MongoCollection.ID_KEY;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.PREFIX;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.encodeKey;

import com.mongodb.BasicDBObject;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.type.GroupTypeExpression;
import org.hypertrace.core.documentstore.parser.GroupTypeExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SelectionSpec;

public final class MongoGroupTypeExpressionParser implements GroupTypeExpressionVisitor {

  private static final String GROUP_CLAUSE = "$group";

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
    String identifier = new MongoIdentifierExpressionParser().parse(expression);
    String key = encodeKey(identifier);
    return Map.of(key, PREFIX + identifier);
  }

  public static BasicDBObject getGroupClause(final Query query) {
    final List<SelectionSpec> selectionSpecs = query.getSelections();
    final List<GroupTypeExpression> expressions = query.getAggregations();

    MongoGroupTypeExpressionParser parser = new MongoGroupTypeExpressionParser();
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

    MongoSelectTypeExpressionParser baseParser = new MongoAggregateExpressionParser();

    Map<String, Object> definition =
        selectionSpecs.stream()
            .map(spec -> MongoGroupTypeExpressionParser.parse(baseParser, spec))
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
      final MongoSelectTypeExpressionParser baseParser, final SelectionSpec spec) {
    SelectTypeExpressionVisitor parser =
        new MongoProjectingParser(spec.getAlias(), baseParser);
    return spec.getExpression().accept(parser);
  }

  private Map<String, Object> parse(final GroupTypeExpression expression) {
    MongoGroupTypeExpressionParser parser = new MongoGroupTypeExpressionParser();
    return expression.accept(parser);
  }
}
