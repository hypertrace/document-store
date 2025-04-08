package org.hypertrace.core.documentstore.mongo.query.parser;

import com.mongodb.BasicDBObject;
import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.AliasedIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayRelationalFilterExpression;
import org.hypertrace.core.documentstore.expression.impl.DocumentArrayFilterExpression;
import org.hypertrace.core.documentstore.expression.impl.KeyExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.parser.FilterTypeExpressionVisitor;

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
    if (expression.getLhs() instanceof AliasedIdentifierExpression) {
      letClause.putAll(
          (Map<String, Object>)
              createLetClause((AliasedIdentifierExpression) expression.getLhs(), subQueryAlias));
    }
    if (expression.getRhs() instanceof AliasedIdentifierExpression) {
      letClause.putAll(
          (Map<String, Object>)
              createLetClause((AliasedIdentifierExpression) expression.getRhs(), subQueryAlias));
    }
    return letClause;
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
