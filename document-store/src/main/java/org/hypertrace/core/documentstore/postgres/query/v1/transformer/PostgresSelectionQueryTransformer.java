package org.hypertrace.core.documentstore.postgres.query.v1.transformer;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.SubQueryIdentifierExpression;
import org.hypertrace.core.documentstore.expression.type.GroupTypeExpression;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.parser.GroupTypeExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SelectionSpec;
import org.hypertrace.core.documentstore.query.transform.QueryTransformer;
import org.hypertrace.core.documentstore.query.transform.TransformedQueryBuilder;

/*
 * Postgres doesn't support the selection of attributes and aggregation w/o group by expression.
 * e.g
 * SELECT COUNT(DISTINCT document->>'quantity' ) AS QTY, document->'price' AS price
 * FROM testCollection
 * WHERE (CAST (document->>'price' AS NUMERIC) <= 10)
 *
 * So, if group by clause is missing, and selection contains any aggregation expression,
 * this transformer removes all the non-aggregated expressions. So, the above query will be transformed
 * to:
 *
 * SELECT COUNT(DISTINCT document->>'quantity' ) AS QTY
 * FROM testCollection
 * WHERE (CAST (document->>'price' AS NUMERIC) <= 10)
 *
 * This is the similar behavior supported in our other document store implementation (e.g Mongo)
 * */
public class PostgresSelectionQueryTransformer implements QueryTransformer {

  @Override
  public Query transform(Query query) {
    LocalSelectTypeAggregationExpressionSelector aggregationExpressionSelector =
        new LocalSelectTypeAggregationExpressionSelector();
    Boolean noAggregation =
        query.getSelections().stream()
            .filter(
                selectionSpec ->
                    selectionSpec.getExpression().accept(aggregationExpressionSelector))
            .findAny()
            .isEmpty();
    if (noAggregation) return query;

    // get all identifier of group by clause
    LocalGroupByIdentifierExpressionSelector groupByIdentifierExpressionSelector =
        new LocalGroupByIdentifierExpressionSelector();
    Set<GroupTypeExpression> groupByIdentifierExpressions =
        query.getAggregations().stream()
            .filter(exp -> exp.accept(groupByIdentifierExpressionSelector))
            .collect(Collectors.toUnmodifiableSet());

    // keep only matching identifier expression with group by along with rest.
    LocalSelectTypeIdentifierExpressionSelector identifierExpressionSelector =
        new LocalSelectTypeIdentifierExpressionSelector();
    List<SelectionSpec> finalSelectionSpecs =
        query.getSelections().stream()
            .filter(
                getSelectionSpecPredicate(
                    groupByIdentifierExpressions, identifierExpressionSelector))
            .collect(Collectors.toUnmodifiableList());

    return finalSelectionSpecs.size() != query.getSelections().size()
        ? new TransformedQueryBuilder(query).setSelections(finalSelectionSpecs).build()
        : query;
  }

  private Predicate<SelectionSpec> getSelectionSpecPredicate(
      Set<GroupTypeExpression> groupByIdentifierExpressions,
      LocalSelectTypeIdentifierExpressionSelector identifierExpressionSelector) {
    return selectionSpec -> {
      SelectTypeExpression expression = selectionSpec.getExpression();
      return (!(boolean) expression.accept(identifierExpressionSelector))
          || groupByIdentifierExpressions.contains(expression);
    };
  }

  private static class LocalSelectTypeAggregationExpressionSelector
      implements SelectTypeExpressionVisitor {
    @Override
    public Boolean visit(AggregateExpression expression) {
      return true;
    }

    @Override
    public Boolean visit(ConstantExpression expression) {
      return false;
    }

    @Override
    public Boolean visit(FunctionExpression expression) {
      return false;
    }

    @Override
    public Boolean visit(IdentifierExpression expression) {
      return false;
    }

    @Override
    public Boolean visit(SubQueryIdentifierExpression expression) {
      return false;
    }
  }

  private static class LocalSelectTypeIdentifierExpressionSelector
      implements SelectTypeExpressionVisitor {
    @Override
    public Boolean visit(AggregateExpression expression) {
      return false;
    }

    @Override
    public Boolean visit(ConstantExpression expression) {
      return false;
    }

    @Override
    public Boolean visit(FunctionExpression expression) {
      return false;
    }

    @Override
    public Boolean visit(IdentifierExpression expression) {
      return true;
    }

    @Override
    public Boolean visit(SubQueryIdentifierExpression expression) {
      return false;
    }
  }

  private static class LocalGroupByIdentifierExpressionSelector
      implements GroupTypeExpressionVisitor {
    @Override
    public Boolean visit(FunctionExpression expression) {
      return false;
    }

    @Override
    public Boolean visit(IdentifierExpression expression) {
      return true;
    }
  }
}
