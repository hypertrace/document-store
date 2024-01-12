package org.hypertrace.core.documentstore.mongo.query.parser.filter;

import java.util.Map;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactory.MongoRelationalFilterContext;

@AllArgsConstructor
public class MongoExistsRelationalFilterParser implements MongoRelationalFilterParser {

  static final String EXISTS = "$exists";

  @Override
  public Map<String, Object> parse(
      final RelationalExpression expression, final MongoRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    final boolean parsedRhs = !ConstantExpression.of(false).equals(expression.getRhs());
    return Map.of(parsedLhs, Map.of(EXISTS, parsedRhs));
  }
}
