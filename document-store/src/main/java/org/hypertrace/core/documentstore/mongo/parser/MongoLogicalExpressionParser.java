package org.hypertrace.core.documentstore.mongo.parser;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.parser.FilteringExpressionVisitor;
import org.hypertrace.core.documentstore.query.Query;

final class MongoLogicalExpressionParser extends MongoExpressionParser {

  MongoLogicalExpressionParser(Query query) {
    super(query);
  }

  Map<String, Object> parse(final LogicalExpression expression) {
    FilteringExpressionVisitor parser = new MongoFilteringExpressionParser(query);
    List<Object> parsed =
        expression.getOperands().stream()
            .map(exp -> exp.visit(parser))
            .collect(Collectors.toList());
    String key = "$" + expression.getOperator().name().toLowerCase();
    return Map.of(key, parsed);
  }
}
