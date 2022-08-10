package org.hypertrace.core.documentstore.postgres;

import static org.hypertrace.core.documentstore.Collection.UNSUPPORTED_QUERY_OPERATION;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.CREATED_AT;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.ID;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.UPDATED_AT;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.OrderBy;
import org.hypertrace.core.documentstore.postgres.Params.Builder;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;

class PostgresQueryParser {

  private static final String QUESTION_MARK = "?";
  // postgres jsonb uses `->` instead of `.` for json field access
  private static final String JSON_FIELD_ACCESSOR = "->";
  // postgres operator to fetch the value of json object as text.
  private static final String JSON_DATA_ACCESSOR = "->>";
  private static final Set<String> OUTER_COLUMNS =
      new HashSet<>() {
        {
          add(CREATED_AT);
          add(ID);
          add(UPDATED_AT);
        }
      };

  static String parseFilter(Filter filter, Builder paramsBuilder) {
    if (filter.isComposite()) {
      return parseCompositeFilter(filter, paramsBuilder);
    } else {
      return parseNonCompositeFilter(filter, paramsBuilder);
    }
  }

  static String parseNonCompositeFilter(Filter filter, Builder paramsBuilder) {
    Filter.Op op = filter.getOp();
    Object value = filter.getValue();
    String fieldName = filter.getFieldName();
    String fullFieldName =
        prepareCast(
            PostgresUtils.prepareFieldDataAccessorExpr(fieldName, PostgresUtils.DOCUMENT_COLUMN),
            value);
    StringBuilder filterString = new StringBuilder(fullFieldName);
    String sqlOperator;
    Boolean isMultiValued = false;
    switch (op) {
      case EQ:
        sqlOperator = " = ";
        break;
      case GT:
        sqlOperator = " > ";
        break;
      case LT:
        sqlOperator = " < ";
        break;
      case GTE:
        sqlOperator = " >= ";
        break;
      case LTE:
        sqlOperator = " <= ";
        break;
      case LIKE:
        // Case insensitive regex search, Append % at beginning and end of value to do a regex
        // search
        sqlOperator = " ILIKE ";
        value = "%" + value + "%";
        break;
      case NOT_IN:
        // NOTE: Below two points
        // 1. both NOT_IN and IN filter currently limited to non-array field
        //    - https://github.com/hypertrace/document-store/issues/32#issuecomment-781411676
        // 2. To make semantically opposite filter of IN, we need to check for if key is not present
        //    Ref in context of NEQ -
        // https://github.com/hypertrace/document-store/pull/20#discussion_r547101520Other
        //    so, we need - "document->key IS NULL OR document->key->> NOT IN (v1, v2)"
        StringBuilder notInFilterString =
            PostgresUtils.prepareFieldAccessorExpr(fieldName, PostgresUtils.DOCUMENT_COLUMN);
        if (notInFilterString != null && !OUTER_COLUMNS.contains(fieldName)) {
          filterString = notInFilterString.append(" IS NULL OR ").append(fullFieldName);
        }
        sqlOperator = " NOT IN ";
        isMultiValued = true;
        value = prepareParameterizedStringForList((List<Object>) value, paramsBuilder);
        break;
      case IN:
        // NOTE: both NOT_IN and IN filter currently limited to non-array field
        //  - https://github.com/hypertrace/document-store/issues/32#issuecomment-781411676
        sqlOperator = " IN ";
        isMultiValued = true;
        value = prepareParameterizedStringForList((List<Object>) value, paramsBuilder);
        break;
      case NOT_EXISTS:
        sqlOperator = " IS NULL ";
        value = null;
        // For fields inside jsonb
        StringBuilder notExists =
            PostgresUtils.prepareFieldAccessorExpr(fieldName, PostgresUtils.DOCUMENT_COLUMN);
        if (notExists != null && !OUTER_COLUMNS.contains(fieldName)) {
          filterString = notExists;
        }
        break;
      case EXISTS:
        sqlOperator = " IS NOT NULL ";
        value = null;
        // For fields inside jsonb
        StringBuilder exists =
            PostgresUtils.prepareFieldAccessorExpr(fieldName, PostgresUtils.DOCUMENT_COLUMN);
        if (exists != null && !OUTER_COLUMNS.contains(fieldName)) {
          filterString = exists;
        }
        break;
      case NEQ:
        sqlOperator = " != ";
        // https://github.com/hypertrace/document-store/pull/20#discussion_r547101520
        // The expected behaviour is to get all documents which either satisfy non equality
        // condition
        // or the key doesn't exist in them
        // Semantics for handling if key not exists and if it exists, its value
        // doesn't equal to the filter for Jsonb document will be done as:
        // "document->key IS NULL OR document->key->> != value"
        StringBuilder notEquals =
            PostgresUtils.prepareFieldAccessorExpr(fieldName, PostgresUtils.DOCUMENT_COLUMN);
        // For fields inside jsonb
        if (notEquals != null && !OUTER_COLUMNS.contains(fieldName)) {
          filterString = notEquals.append(" IS NULL OR ").append(fullFieldName);
        }
        break;
      case CONTAINS:
        // TODO: Matches condition inside an array of documents
      default:
        throw new UnsupportedOperationException(UNSUPPORTED_QUERY_OPERATION);
    }

    filterString.append(sqlOperator);
    if (value != null) {
      if (isMultiValued) {
        filterString.append(value);
      } else {
        filterString.append(QUESTION_MARK);
        paramsBuilder.addObjectParam(value);
      }
    }
    String filters = filterString.toString();
    return filters;
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

  private static String prepareParameterizedStringForList(
      List<Object> values, Params.Builder paramsBuilder) {
    String collect =
        values.stream()
            .map(
                val -> {
                  paramsBuilder.addObjectParam(val);
                  return QUESTION_MARK;
                })
            .collect(Collectors.joining(", "));
    return "(" + collect + ")";
  }

  private static String prepareCast(String field, Object value) {
    String fmt = "CAST (%s AS %s)";

    // handle the case if the value type is collection for filter operator - `IN`
    // Currently, for `IN` operator, we are considering List collection, and it is fair
    // assumption that all its value of the same types. Based on that and for consistency
    // we will use CAST ( <field name> as <type> ) for all non string operator.
    // Ref : https://github.com/hypertrace/document-store/pull/30#discussion_r571782575

    if (value instanceof List<?> && ((List<Object>) value).size() > 0) {
      List<Object> listValue = (List<Object>) value;
      value = listValue.get(0);
    }

    if (value instanceof Number) {
      return String.format(fmt, field, "NUMERIC");
    } else if (value instanceof Boolean) {
      return String.format(fmt, field, "BOOLEAN");
    } else /* default is string */ {
      return field;
    }
  }
}
