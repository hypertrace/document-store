package org.hypertrace.core.documentstore.mongo.query.parser;

import static java.util.Collections.unmodifiableMap;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.AND;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.NOT;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.OR;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.getUnsupportedOperationException;
import static org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactory.FilterLocation.INSIDE_EXPR;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.operators.LogicalOperator;
import org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactory.MongoRelationalFilterContext;
import org.hypertrace.core.documentstore.parser.FilterTypeExpressionVisitor;

final class MongoLogicalExpressionParser {

  private static final String NOR_OP = "$nor";
  private static final String NOT_OP = "$not";

  private static final Map<LogicalOperator, String> KEY_MAP =
      unmodifiableMap(
          new EnumMap<>(LogicalOperator.class) {
            {
              put(AND, "$and");
              put(OR, "$or");
            }
          });

  private final MongoRelationalFilterContext relationalFilterContext;

  MongoLogicalExpressionParser(final MongoRelationalFilterContext relationalFilterContext) {
    this.relationalFilterContext = relationalFilterContext;
  }

  Map<String, Object> parse(final LogicalExpression expression) {
    LogicalOperator operator = expression.getOperator();
    String key = NOT.equals(operator) ? getNotOp() : KEY_MAP.get(operator);

    if (key == null) {
      throw getUnsupportedOperationException(operator);
    }

    FilterTypeExpressionVisitor parser =
        new MongoFilterTypeExpressionParser(relationalFilterContext);

    List<Object> parsed =
        expression.getOperands().stream()
            .map(exp -> exp.accept(parser))
            .collect(Collectors.toList());

    return Map.of(key, parsed);
  }

  private String getNotOp() {
    return INSIDE_EXPR.equals(relationalFilterContext.location()) ? NOT_OP : NOR_OP;
  }
}
