package org.hypertrace.core.documentstore.mongo.query.parser;

import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactory;
import org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactory.MongoRelationalFilterContext;
import org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactoryImpl;

final class MongoRelationalExpressionParser {
  private static final MongoRelationalFilterParserFactory factory =
      new MongoRelationalFilterParserFactoryImpl();

  Map<String, Object> parse(
      final RelationalExpression expression, final MongoRelationalFilterContext context) {
    return factory.parser(expression, context).parse(expression, context);
  }
}
