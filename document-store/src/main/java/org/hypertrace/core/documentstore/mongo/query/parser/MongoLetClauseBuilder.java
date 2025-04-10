package org.hypertrace.core.documentstore.mongo.query.parser;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.AliasedIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayRelationalFilterExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.DocumentArrayFilterExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.KeyExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.mongo.MongoUtils;
import org.hypertrace.core.documentstore.parser.FilterTypeExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;

// Visitor for building the let clause in lookup stage from filter expressions
class MongoLetClauseBuilder implements FilterTypeExpressionVisitor {

  private final String subQueryAlias;

  MongoLetClauseBuilder(String subQueryAlias) {
    this.subQueryAlias = subQueryAlias;
  }

  @Override
  public Map<String, Object> visit(LogicalExpression expression) {
    Map<String, Object> letClause = new HashMap<>();
    for (FilterTypeExpression operand : expression.getOperands()) {
      letClause.putAll(operand.accept(this));
    }
    return Collections.unmodifiableMap(letClause);
  }

  @Override
  public Map<String, Object> visit(RelationalExpression expression) {
    Map<String, Object> letClause = new HashMap<>();
    letClause.putAll(expression.getLhs().accept(new MongoLetClauseSelectTypeExpressionVisitor()));
    letClause.putAll(expression.getRhs().accept(new MongoLetClauseSelectTypeExpressionVisitor()));
    return Collections.unmodifiableMap(letClause);
  }

  private class MongoLetClauseSelectTypeExpressionVisitor implements SelectTypeExpressionVisitor {
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
      return createLetClause(expression, subQueryAlias);
    }

    private Map<String, Object> createLetClause(
        AliasedIdentifierExpression aliasedExpression, String subQueryAlias) {
      Map<String, Object> letClause = new HashMap<>();
      if (aliasedExpression.getContextAlias().equals(subQueryAlias)) {
        letClause.put(aliasedExpression.getName(), MongoUtils.PREFIX + aliasedExpression.getName());
      }
      return Collections.unmodifiableMap(letClause);
    }
  }

  @Override
  public Map<String, Object> visit(KeyExpression expression) {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, Object> visit(ArrayRelationalFilterExpression expression) {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, Object> visit(DocumentArrayFilterExpression expression) {
    return Collections.emptyMap();
  }
}
