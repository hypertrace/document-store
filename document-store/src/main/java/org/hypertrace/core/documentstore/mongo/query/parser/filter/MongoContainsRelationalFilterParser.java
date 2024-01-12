package org.hypertrace.core.documentstore.mongo.query.parser.filter;

import com.mongodb.BasicDBObject;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactory.MongoRelationalFilterContext;

@AllArgsConstructor
public class MongoContainsRelationalFilterParser implements MongoRelationalFilterParser {

  private static final String ELEM_MATCH = "$elemMatch";
  private static final String EQ = "$eq";

  @Override
  public Map<String, Object> parse(
      final RelationalExpression expression, final MongoRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    final Object parsedRhs = expression.getRhs().accept(context.rhsParser());
    return Map.of(parsedLhs, buildElemMatch(parsedRhs));
  }

  static BasicDBObject buildElemMatch(final Object parsedRhs) {
    return new BasicDBObject(ELEM_MATCH, new BasicDBObject(EQ, parsedRhs));
  }
}
