package org.hypertrace.core.documentstore.mongo.query.parser.filter;

import static org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoContainsRelationalFilterParser.buildElemMatch;

import java.util.Map;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactory.MongoRelationalFilterContext;

@AllArgsConstructor
public class MongoNotContainsRelationalFilterParser implements MongoRelationalFilterParser {
  private static final String NOT = "$not";

  @Override
  public Map<String, Object> parse(
      final RelationalExpression expression, final MongoRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    final Object parsedRhs = expression.getRhs().accept(context.rhsParser());
    return Map.of(parsedLhs, Map.of(NOT, buildElemMatch(parsedRhs)));
  }
}
