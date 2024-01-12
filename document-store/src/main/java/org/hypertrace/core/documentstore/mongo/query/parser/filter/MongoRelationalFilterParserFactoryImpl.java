package org.hypertrace.core.documentstore.mongo.query.parser.filter;

import static org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactory.FilterLocation.INSIDE_EXPR;
import static org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactory.FilterLocation.OUTSIDE_EXPR;

import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;

public class MongoRelationalFilterParserFactoryImpl implements MongoRelationalFilterParserFactory {

  @Override
  public MongoRelationalFilterParser parser(
      final RelationalExpression expression, final MongoRelationalFilterContext context) {
    switch (expression.getOperator()) {
      case EQ:
      case NEQ:
      case GT:
      case LT:
      case GTE:
      case LTE:
        if (INSIDE_EXPR.equals(context.location())) {
          return new MongoStandardExprRelationalFilterParser();
        } else if (OUTSIDE_EXPR.equals(context.location())) {
          return new MongoFunctionExprRelationalFilterParser();
        } else {
          throw new UnsupportedOperationException("Unsupported location: " + context.location());
        }

      case IN:
      case NOT_IN:
        return new MongoStandardNonExprRelationalFilterParser();

      case CONTAINS:
        return new MongoContainsRelationalFilterParser();

      case NOT_CONTAINS:
        return new MongoNotContainsRelationalFilterParser();

      case EXISTS:
        return new MongoExistsRelationalFilterParser();

      case NOT_EXISTS:
        return new MongoNotExistsRelationalFilterParser();

      case LIKE:
        return new MongoLikeRelationalFilterParser();

      case STARTS_WITH:
        return new MongoStartsWithRelationalFilterParser();
    }

    throw new UnsupportedOperationException("Unsupported operator: " + expression.getOperator());
  }
}
