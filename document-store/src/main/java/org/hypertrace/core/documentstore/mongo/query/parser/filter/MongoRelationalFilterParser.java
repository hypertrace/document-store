package org.hypertrace.core.documentstore.mongo.query.parser.filter;

import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactory.MongoRelationalFilterContext;

public interface MongoRelationalFilterParser {
  Map<String, Object> parse(
      final RelationalExpression expression, final MongoRelationalFilterContext context);
}
