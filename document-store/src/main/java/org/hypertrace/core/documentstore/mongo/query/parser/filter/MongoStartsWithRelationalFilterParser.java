package org.hypertrace.core.documentstore.mongo.query.parser.filter;

import com.mongodb.BasicDBObject;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactory.MongoRelationalFilterContext;

@AllArgsConstructor
public class MongoStartsWithRelationalFilterParser implements MongoRelationalFilterParser {
  private static final String REGEX = "$regex";
  private static final String STARTS_WITH_PREFIX = "^";

  @Override
  public Map<String, Object> parse(
      RelationalExpression expression, final MongoRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    final Object parsedRhs = expression.getRhs().accept(context.rhsParser());
    // Since starts with makes sense only for string values, the RHS is converted to a string
    final String rhsValue = STARTS_WITH_PREFIX + parsedRhs;
    return Map.of(parsedLhs, new BasicDBObject(REGEX, rhsValue));
  }
}
