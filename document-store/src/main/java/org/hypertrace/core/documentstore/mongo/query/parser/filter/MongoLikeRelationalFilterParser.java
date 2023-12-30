package org.hypertrace.core.documentstore.mongo.query.parser.filter;

import com.mongodb.BasicDBObject;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactory.MongoRelationalFilterContext;

@AllArgsConstructor
public class MongoLikeRelationalFilterParser implements MongoRelationalFilterParser {
  private static final String IGNORE_CASE_OPTION = "i";
  private static final String OPTIONS = "$options";
  private static final String REGEX = "$regex";

  @Override
  public Map<String, Object> parse(
      final RelationalExpression expression, final MongoRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    final Object parsedRhs = expression.getRhs().accept(context.rhsParser());
    return Map.of(
        parsedLhs, new BasicDBObject(REGEX, parsedRhs).append(OPTIONS, IGNORE_CASE_OPTION));
  }
}
