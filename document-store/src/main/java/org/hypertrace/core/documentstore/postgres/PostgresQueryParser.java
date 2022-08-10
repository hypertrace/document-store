package org.hypertrace.core.documentstore.postgres;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.OrderBy;
import org.hypertrace.core.documentstore.postgres.Params.Builder;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;

class PostgresQueryParser {

  static String parseSelections(List<String> selections) {
    return Optional.of(
            selections.stream()
                .map(
                    selection ->
                        String.format(
                            "%s AS \"%s\"",
                            PostgresUtils.prepareFieldAccessorExpr(
                                selection, PostgresUtils.DOCUMENT_COLUMN),
                            selection))
                .collect(Collectors.joining(",")))
        .filter(str -> StringUtils.isNotBlank(str))
        .orElse("*");
  }

  static String parseFilter(Filter filter, Builder paramsBuilder) {
    if (filter.isComposite()) {
      return parseCompositeFilter(filter, paramsBuilder);
    } else {
      return PostgresUtils.parseNonCompositeFilter(
          filter.getFieldName(),
          PostgresUtils.DOCUMENT_COLUMN,
          filter.getOp().toString(),
          filter.getValue(),
          paramsBuilder);
    }
  }

  static String parseCompositeFilter(Filter filter, Builder paramsBuilder) {
    Filter.Op op = filter.getOp();
    switch (op) {
      case OR:
        {
          String childList =
              Arrays.stream(filter.getChildFilters())
                  .map(childFilter -> parseFilter(childFilter, paramsBuilder))
                  .filter(str -> !StringUtils.isEmpty(str))
                  .map(str -> "(" + str + ")")
                  .collect(Collectors.joining(" OR "));
          return !childList.isEmpty() ? childList : null;
        }
      case AND:
        {
          String childList =
              Arrays.stream(filter.getChildFilters())
                  .map(childFilter -> parseFilter(childFilter, paramsBuilder))
                  .filter(str -> !StringUtils.isEmpty(str))
                  .map(str -> "(" + str + ")")
                  .collect(Collectors.joining(" AND "));
          return !childList.isEmpty() ? childList : null;
        }
      default:
        throw new UnsupportedOperationException(
            String.format("Query operation:%s not supported", op));
    }
  }

  static String parseOrderBys(List<OrderBy> orderBys) {
    return orderBys.stream()
        .map(
            orderBy ->
                PostgresUtils.prepareFieldDataAccessorExpr(
                        orderBy.getField(), PostgresUtils.DOCUMENT_COLUMN)
                    + " "
                    + (orderBy.isAsc() ? "ASC" : "DESC"))
        .filter(str -> !StringUtils.isEmpty(str))
        .collect(Collectors.joining(" , "));
  }
}
