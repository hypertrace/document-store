package org.hypertrace.core.documentstore.mongo.parser;

import java.util.Map;
import java.util.Set;
import lombok.Builder;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.impl.KeyExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.parser.FilterTypeExpressionVisitor;

public final class MongoJoinConditionParser implements FilterTypeExpressionVisitor {

  @SuppressWarnings("unchecked")
  @Override
  public MongoJoinParseResult visit(final LogicalExpression expression) {
    return new MongoLogicalJoinConditionParser().parse(expression);
  }

  @SuppressWarnings("unchecked")
  @Override
  public MongoJoinParseResult visit(final RelationalExpression expression) {
    return new MongoRelationalJoinConditionParser().parse(expression);
  }

  @SuppressWarnings("unchecked")
  @Override
  public MongoJoinParseResult visit(final KeyExpression expression) {
    throw new UnsupportedOperationException("KeyExpression cannot be a part of the join condition");
  }

  public static MongoJoinParseResult getFilter(final FilterTypeExpression filterTypeExpression) {
    final FilterTypeExpressionVisitor parser = new MongoJoinConditionParser();
    return filterTypeExpression.accept(parser);
  }

  @Value
  @Builder
  static class MongoJoinParseResult {
    Set<String> variables;
    Map<String, Object> parsedFilter;
  }
}
