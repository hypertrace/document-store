package org.hypertrace.core.documentstore.postgres.query.v1.transformer;

import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
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
public class PostgresSelectionQueryTransformer
    implements QueryTransformer, SelectTypeExpressionVisitor {

  @Override
  public Query transform(Query query) {
    // no-op if group by clause exits
    if (!query.getAggregations().isEmpty()) return query;

    // check for all selections, remove non-aggregated selections.
    List<SelectionSpec> finalSelectionSpecs =
        query.getSelections().stream()
            .filter(selectionSpec -> selectionSpec.getExpression().accept(this))
            .collect(Collectors.toUnmodifiableList());

    return finalSelectionSpecs.size() > 0
        ? new TransformedQueryBuilder(query).setSelections(finalSelectionSpecs).build()
        : query;
  }

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
