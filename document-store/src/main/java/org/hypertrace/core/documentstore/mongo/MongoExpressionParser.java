package org.hypertrace.core.documentstore.mongo;

import org.hypertrace.core.documentstore.expression.Expression;

public abstract class MongoExpressionParser<T extends Expression> {
  protected final T expression;

  public MongoExpressionParser(T expression) {
    this.expression = expression;
  }

  public abstract Object parse();
}
