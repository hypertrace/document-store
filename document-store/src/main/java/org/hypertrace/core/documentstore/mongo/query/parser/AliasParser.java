package org.hypertrace.core.documentstore.mongo.query.parser;

import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression.DocumentConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SortTypeExpressionVisitor;

public class AliasParser implements SelectTypeExpressionVisitor, SortTypeExpressionVisitor {

  @Override
  public String visit(AggregateExpression expression) {
    return null;
  }

  @Override
  public String visit(FunctionExpression expression) {
    return null;
  }

  @Override
  public String visit(IdentifierExpression expression) {
    return expression.getName();
  }

  @Override
  public String visit(ConstantExpression expression) {
    return null;
  }

  @Override
  public String visit(DocumentConstantExpression expression) {
    return null;
  }
}
