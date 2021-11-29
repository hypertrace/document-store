package org.hypertrace.core.documentstore.query.transform;

import java.util.List;
import org.apache.commons.collections4.CollectionUtils;
import org.hypertrace.core.documentstore.query.QueryBuilder;
import org.hypertrace.core.documentstore.query.QueryInternal;
import org.hypertrace.core.documentstore.query.SelectionSpec;

public final class TransformedQueryBuilder extends QueryBuilder {

  public TransformedQueryBuilder(final QueryInternal query) {
    copySelections(query);
    copyFilter(query);

    copyAggregation(query);
    copyAggregationFilter(query);

    copySorts(query);
    copyPagination(query);
  }

  public QueryBuilder replaceSelections(final List<SelectionSpec> newSpecs) {
    if (CollectionUtils.isNotEmpty(newSpecs)) {
      getSelectionBuilder().clearSelectionSpecs().selectionSpecs(newSpecs);
    }
    return this;
  }

  private void copySelections(final QueryInternal query) {
    // Not invoking "addSelections()" since we will get an immutable list
    query.getSelections().forEach(this::addSelection);
  }

  private void copyFilter(final QueryInternal query) {
    query.getFilter().ifPresent(this::setFilter);
  }

  private void copyAggregation(final QueryInternal query) {
    // Not invoking "addAggregations()" since we will get an immutable list
    query.getAggregations().forEach(this::addAggregation);
  }

  private void copyAggregationFilter(final QueryInternal query) {
    query.getAggregationFilter().ifPresent(this::setAggregationFilter);
  }

  private void copySorts(final QueryInternal query) {
    // Not invoking "addSorts()" since we will get an immutable list
    query.getSorts().forEach(this::addSort);
  }

  private void copyPagination(final QueryInternal query) {
    query.getPagination().ifPresent(this::setPagination);
  }
}
