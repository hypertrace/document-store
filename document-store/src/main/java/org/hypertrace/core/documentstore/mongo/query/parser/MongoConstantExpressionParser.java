package org.hypertrace.core.documentstore.mongo.query.parser;

import static org.hypertrace.core.documentstore.mongo.MongoUtils.sanitizeJsonString;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.BasicDBObject;
import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression.DocumentConstantExpression;

@NoArgsConstructor
final class MongoConstantExpressionParser extends MongoSelectTypeExpressionParser {
  MongoConstantExpressionParser(final MongoSelectTypeExpressionParser baseParser) {
    super(baseParser);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object visit(final ConstantExpression expression) {
    return parse(expression);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object visit(final DocumentConstantExpression expression) {
    try {
      return BasicDBObject.parse(sanitizeJsonString(expression.getValue().toJson()));
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  Object parse(final ConstantExpression expression) {
    return expression.getValue();
  }
}
