package org.hypertrace.core.documentstore.query.transform;

import org.hypertrace.core.documentstore.query.QueryBuilder;
import org.hypertrace.core.documentstore.query.QueryInternal;

public final class TransformedQueryBuilder extends QueryBuilder {

  public TransformedQueryBuilder(final QueryInternal query) {
    copySelections(query);
    copyFilter(query);

    copyAggregations(query);
    copyAggregationFilter(query);

    copySorts(query);
    copyPagination(query);
  }

  private void copySelections(final QueryInternal query) {
    // Iterate through elements to ensure deep-copy
    query.getSelections().forEach(this::addSelection);
  }

  private void copyFilter(final QueryInternal query) {
    query.getFilter().ifPresent(this::setFilter);
  }

  private void copyAggregations(final QueryInternal query) {
    // Iterate through elements to ensure deep-copy
    query.getAggregations().forEach(this::addAggregation);
  }

  private void copyAggregationFilter(final QueryInternal query) {
    query.getAggregationFilter().ifPresent(this::setAggregationFilter);
  }

  private void copySorts(final QueryInternal query) {
    // Iterate through elements to ensure deep-copy
    query.getSorts().forEach(this::addSort);
  }

  private void copyPagination(final QueryInternal query) {
    query.getPagination().ifPresent(this::setPagination);
  }
}
