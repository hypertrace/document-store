package org.hypertrace.core.documentstore.mongo.query.parser;

import com.mongodb.BasicDBObject;
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
import org.hypertrace.core.documentstore.parser.FilterTypeExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;

// Visitor for building the let clause in lookup stage from filter expressions
class MongoLetClauseBuilder implements FilterTypeExpressionVisitor {

  private final String subQueryAlias;

  MongoLetClauseBuilder(String subQueryAlias) {
    this.subQueryAlias = subQueryAlias;
  }

  @Override
  public BasicDBObject visit(LogicalExpression expression) {
    BasicDBObject letClause = new BasicDBObject();
    for (FilterTypeExpression operand : expression.getOperands()) {
      letClause.putAll((Map<String, Object>) operand.accept(this));
    }
    return letClause;
  }

  @Override
  public BasicDBObject visit(RelationalExpression expression) {
    BasicDBObject letClause = new BasicDBObject();
    letClause.putAll(
        (Map<String, Object>)
            expression.getLhs().accept(new MongoLetClauseSelectTypeExpressionVisitor()));
    letClause.putAll(
        (Map<String, Object>)
            expression.getRhs().accept(new MongoLetClauseSelectTypeExpressionVisitor()));
    return letClause;
  }

  private class MongoLetClauseSelectTypeExpressionVisitor implements SelectTypeExpressionVisitor {
    @Override
    public BasicDBObject visit(AggregateExpression expression) {
      return new BasicDBObject();
    }

    @Override
    public BasicDBObject visit(ConstantExpression expression) {
      return new BasicDBObject();
    }

    @Override
    public BasicDBObject visit(ConstantExpression.DocumentConstantExpression expression) {
      return new BasicDBObject();
    }

    @Override
    public BasicDBObject visit(FunctionExpression expression) {
      return new BasicDBObject();
    }

    @Override
    public BasicDBObject visit(IdentifierExpression expression) {
      return new BasicDBObject();
    }

    @Override
    public BasicDBObject visit(AliasedIdentifierExpression expression) {
      return createLetClause(expression, subQueryAlias);
    }
  }

  @Override
  public BasicDBObject visit(KeyExpression expression) {
    return new BasicDBObject();
  }

  @Override
  public BasicDBObject visit(ArrayRelationalFilterExpression expression) {
    return new BasicDBObject();
  }

  @Override
  public BasicDBObject visit(DocumentArrayFilterExpression expression) {
    return new BasicDBObject();
  }

  private BasicDBObject createLetClause(
      AliasedIdentifierExpression aliasedExpression, String subQueryAlias) {
    BasicDBObject letClause = new BasicDBObject();
    if (aliasedExpression.getAlias().equals(subQueryAlias)) {
      letClause.put(aliasedExpression.getName(), "$" + aliasedExpression.getName());
    }
    return letClause;
  }
}
