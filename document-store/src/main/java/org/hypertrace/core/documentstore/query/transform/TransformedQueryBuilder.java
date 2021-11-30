package org.hypertrace.core.documentstore.query.transform;

import org.hypertrace.core.documentstore.query.Query;

public final class TransformedQueryBuilder extends Query.QueryBuilder {

  public TransformedQueryBuilder(final Query query) {
    copySelections(query);
    copyFilter(query);

    copyAggregations(query);
    copyAggregationFilter(query);

    copySorts(query);
    copyPagination(query);
  }

  private void copySelections(final Query query) {
    // Iterate through elements to ensure deep-copy
    query.getSelections().forEach(this::addSelection);
  }

  private void copyFilter(final Query query) {
    query.getFilter().ifPresent(this::setFilter);
  }

  private void copyAggregations(final Query query) {
    // Iterate through elements to ensure deep-copy
    query.getAggregations().forEach(this::addAggregation);
  }

  private void copyAggregationFilter(final Query query) {
    query.getAggregationFilter().ifPresent(this::setAggregationFilter);
  }

  private void copySorts(final Query query) {
    // Iterate through elements to ensure deep-copy
    query.getSorts().forEach(this::addSort);
  }

  private void copyPagination(final Query query) {
    query.getPagination().ifPresent(this::setPagination);
  }
}
