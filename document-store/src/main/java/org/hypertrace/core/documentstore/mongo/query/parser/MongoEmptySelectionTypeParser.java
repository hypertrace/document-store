package org.hypertrace.core.documentstore.mongo.query.parser;

import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import java.util.Collections;
import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.AliasedIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;

class MongoEmptySelectionTypeParser implements SelectTypeExpressionVisitor {
  @Override
  public Map<String, Object> visit(AggregateExpression expression) {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, Object> visit(ConstantExpression expression) {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, Object> visit(ConstantExpression.DocumentConstantExpression expression) {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, Object> visit(FunctionExpression expression) {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, Object> visit(IdentifierExpression expression) {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, Object> visit(AliasedIdentifierExpression expression) {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, Object> visit(JsonIdentifierExpression expression) {
    return visit((IdentifierExpression) expression);
  }
}
