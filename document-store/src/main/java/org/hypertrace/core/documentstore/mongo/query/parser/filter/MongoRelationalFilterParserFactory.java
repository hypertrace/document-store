package org.hypertrace.core.documentstore.mongo.query.parser.filter;

import static org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactory.FilterLocation.OUTSIDE_EXPR;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;
import lombok.experimental.Accessors;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.mongo.query.parser.MongoConstantExpressionParser;
import org.hypertrace.core.documentstore.mongo.query.parser.MongoIdentifierExpressionParser;
import org.hypertrace.core.documentstore.mongo.query.parser.MongoSelectTypeExpressionParser;

public interface MongoRelationalFilterParserFactory {
  MongoRelationalFilterParser parser(
      final RelationalExpression expression, final MongoRelationalFilterContext context);

  @Value
  @Builder
  @Accessors(fluent = true)
  class MongoRelationalFilterContext {
    public static final MongoRelationalFilterContext DEFAULT_INSTANCE =
        MongoRelationalFilterContext.builder().build();

    @Default FilterLocation location = OUTSIDE_EXPR;
    @Default MongoSelectTypeExpressionParser lhsParser = new MongoIdentifierExpressionParser();
    // FIXME: how to parse AliasedIdentifierExpression / IdentifierExpression in rhs?
    @Default MongoSelectTypeExpressionParser rhsParser = new MongoConstantExpressionParser();
  }

  enum FilterLocation {
    INSIDE_EXPR,
    OUTSIDE_EXPR,
    ;
  }
}
