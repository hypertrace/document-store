package org.hypertrace.core.documentstore.mongo.query.parser;

import java.util.Map;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactory;
import org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactory.MongoRelationalFilterContext;
import org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactoryImpl;

@AllArgsConstructor
final class MongoRelationalExpressionParser {
  private static final MongoRelationalFilterParserFactory factory =
      new MongoRelationalFilterParserFactoryImpl();

  private final MongoRelationalFilterContext context;

  Map<String, Object> parse(final RelationalExpression expression) {
    return factory.parser(expression, context).parse(expression, context);
  }
}
