package org.hypertrace.core.documentstore.postgres.query.v1.transformer;

import com.google.common.base.Strings;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
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
    SelectTypeAggregationExpressionChecker selectTypeAggregationExpressionChecker = new SelectTypeAggregationExpressionChecker();
    SelectTypeIdentifierExpressionSelector selectTypeIdentifierExpressionSelector = new SelectTypeIdentifierExpressionSelector();

    Boolean hasAggregationFunction = query.getSelections().stream()
        .filter(selectionSpec -> selectionSpec.getExpression().accept(selectTypeAggregationExpressionChecker))
        .findAny()
        .isEmpty();

    if (!hasAggregationFunction) return query;

    // get all identifier of group by clause
    LocalGroupByExpressionVisitor groupByExpressionVisitor = new LocalGroupByExpressionVisitor();
    Set<String> groupByExpressions = query.getAggregations().stream()
        .map(gte -> (String) gte.accept(groupByExpressionVisitor))
        .filter(s -> !Strings.isNullOrEmpty(s))
        .collect(Collectors.toUnmodifiableSet());

    // check for all selections, remove non-aggregated selections.
    List<SelectionSpec> finalSelectionSpecs =
        query.getSelections().stream()
            .filter(selectionSpec -> (boolean) selectionSpec.getExpression().accept(selectTypeAggregationExpressionChecker)
                && groupByExpressions.contains((String)selectionSpec.getExpression().accept(selectTypeIdentifierExpressionSelector)))
            .collect(Collectors.toUnmodifiableList());

    return finalSelectionSpecs.size() > 0
        ? new TransformedQueryBuilder(query).setSelections(finalSelectionSpecs).build()
        : query;
  }

  private static class SelectTypeAggregationExpressionChecker implements SelectTypeExpressionVisitor {
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
  }

  private static class SelectTypeIdentifierExpressionSelector implements SelectTypeExpressionVisitor {
    @Override
    public String visit(AggregateExpression expression) {
      return null;
    }

    @Override
    public String visit(ConstantExpression expression) {
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
  }

  private static class LocalGroupByExpressionVisitor implements GroupTypeExpressionVisitor{
    @Override
    public String visit(FunctionExpression expression) {
      return null;
    }

    @Override
    public String visit(IdentifierExpression expression) {
      return expression.getName();
    }
  }
}
