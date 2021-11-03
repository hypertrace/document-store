package org.hypertrace.core.documentstore.mongo;

import org.hypertrace.core.documentstore.expression.Expression;

public interface MongoExpressionParser<T extends Expression> {
  Object parseExpression(T expression);
}
