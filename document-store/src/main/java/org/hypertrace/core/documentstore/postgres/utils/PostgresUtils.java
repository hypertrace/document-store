package org.hypertrace.core.documentstore.postgres.utils;

import static org.hypertrace.core.documentstore.Collection.UNSUPPORTED_QUERY_OPERATION;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.CREATED_AT;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.DOCUMENT;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.DOC_PATH_SEPARATOR;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.ID;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.UPDATED_AT;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.postgres.Params.Builder;

public class PostgresUtils {
  private static final String QUESTION_MARK = "?";
  private static final String JSON_FIELD_ACCESSOR = "->";
  private static final String JSON_DATA_ACCESSOR = "->>";
  private static final String DOT_STR = "_dot_";
  private static final String DOT = ".";

  private static final Set<String> OUTER_COLUMNS = Set.of(CREATED_AT, ID, UPDATED_AT);

  public enum Type {
    STRING,
    BOOLEAN,
    NUMERIC,
  }

  public static StringBuilder prepareFieldAccessorExpr(String fieldName) {
    // Generate json field accessor statement
    if (!OUTER_COLUMNS.contains(fieldName)) {
      StringBuilder filterString = new StringBuilder(DOCUMENT);
      String[] nestedFields = fieldName.split(DOC_PATH_SEPARATOR);
      for (String nestedField : nestedFields) {
        filterString.append(JSON_FIELD_ACCESSOR).append("'").append(nestedField).append("'");
      }
      return filterString;
    }
    // Field accessor is only applicable to jsonb fields, return null otherwise
    return null;
  }

  /**
   * Add field prefix for searching into json document based on postgres syntax, handles nested
   * keys. Note: It doesn't handle array elements in json document. e.g SELECT * FROM TABLE where
   * document ->> 'first' = 'name' and document -> 'address' ->> 'pin' = "00000"
   */
  public static String prepareFieldDataAccessorExpr(String fieldName) {
    StringBuilder fieldPrefix = new StringBuilder(fieldName);
    if (!OUTER_COLUMNS.contains(fieldName)) {
      fieldPrefix = new StringBuilder(DOCUMENT);
      String[] nestedFields = fieldName.split(DOC_PATH_SEPARATOR);
      for (int i = 0; i < nestedFields.length - 1; i++) {
        fieldPrefix.append(JSON_FIELD_ACCESSOR).append("'").append(nestedFields[i]).append("'");
      }
      fieldPrefix
          .append(JSON_DATA_ACCESSOR)
          .append("'")
          .append(nestedFields[nestedFields.length - 1])
          .append("'");
    }
    return fieldPrefix.toString();
  }

  public static String prepareCast(String field, Object value) {
    Type type = Type.STRING;

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
      type = Type.NUMERIC;
    } else if (value instanceof Boolean) {
      type = Type.NUMERIC;
    }

    return prepareCast(field, type);
  }

  public static String prepareCast(String field, Type type) {
    String fmt = "CAST (%s AS %s)";
    if (type.equals(Type.NUMERIC)) {
      return String.format(fmt, field, type);
    } else if (type.equals(Type.BOOLEAN)) {
      return String.format(fmt, field, type);
    } else /* default is string */ {
      return field;
    }
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

  public static String parseNonCompositeFilter(
      String fieldName, String op, Object value, Builder paramsBuilder) {
    String fullFieldName = prepareCast(prepareFieldDataAccessorExpr(fieldName), value);
    StringBuilder filterString = new StringBuilder(fullFieldName);
    String sqlOperator;
    Boolean isMultiValued = false;
    switch (op) {
      case "EQ":
        sqlOperator = " = ";
        break;
      case "GT":
        sqlOperator = " > ";
        break;
      case "LT":
        sqlOperator = " < ";
        break;
      case "GTE":
        sqlOperator = " >= ";
        break;
      case "LTE":
        sqlOperator = " <= ";
        break;
      case "LIKE":
        // Case insensitive regex search, Append % at beginning and end of value to do a regex
        // search
        sqlOperator = " ILIKE ";
        value = "%" + value + "%";
        break;
      case "NOT_IN":
        // NOTE: Below two points
        // 1. both NOT_IN and IN filter currently limited to non-array field
        //    - https://github.com/hypertrace/document-store/issues/32#issuecomment-781411676
        // 2. To make semantically opposite filter of IN, we need to check for if key is not present
        //    Ref in context of NEQ -
        // https://github.com/hypertrace/document-store/pull/20#discussion_r547101520Other
        //    so, we need - "document->key IS NULL OR document->key->> NOT IN (v1, v2)"
        StringBuilder notInFilterString = prepareFieldAccessorExpr(fieldName);
        if (notInFilterString != null) {
          filterString = notInFilterString.append(" IS NULL OR ").append(fullFieldName);
        }
        sqlOperator = " NOT IN ";
        isMultiValued = true;
        value = prepareParameterizedStringForList((List<Object>) value, paramsBuilder);
        break;
      case "IN":
        // NOTE: both NOT_IN and IN filter currently limited to non-array field
        //  - https://github.com/hypertrace/document-store/issues/32#issuecomment-781411676
        sqlOperator = " IN ";
        isMultiValued = true;
        value = prepareParameterizedStringForList((List<Object>) value, paramsBuilder);
        break;
      case "NOT_EXISTS":
        sqlOperator = " IS NULL ";
        value = null;
        // For fields inside jsonb
        StringBuilder notExists = prepareFieldAccessorExpr(fieldName);
        if (notExists != null) {
          filterString = notExists;
        }
        break;
      case "EXISTS":
        sqlOperator = " IS NOT NULL ";
        value = null;
        // For fields inside jsonb
        StringBuilder exists = prepareFieldAccessorExpr(fieldName);
        if (exists != null) {
          filterString = exists;
        }
        break;
      case "NEQ":
        sqlOperator = " != ";
        // https://github.com/hypertrace/document-store/pull/20#discussion_r547101520
        // The expected behaviour is to get all documents which either satisfy non equality
        // condition
        // or the key doesn't exist in them
        // Semantics for handling if key not exists and if it exists, its value
        // doesn't equal to the filter for Jsonb document will be done as:
        // "document->key IS NULL OR document->key->> != value"
        StringBuilder notEquals = prepareFieldAccessorExpr(fieldName);
        // For fields inside jsonb
        if (notEquals != null) {
          filterString = notEquals.append(" IS NULL OR ").append(fullFieldName);
        }
        break;
      case "CONTAINS":
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

  public static String prepareParsedNonCompositeFilter(
      String preparedExpression, String op, Object value, Builder paramsBuilder) {
    StringBuilder filterString = new StringBuilder(preparedExpression);
    String sqlOperator;
    Boolean isMultiValued = false;
    switch (op) {
      case "EQ":
        sqlOperator = " = ";
        break;
      case "GT":
        sqlOperator = " > ";
        break;
      case "LT":
        sqlOperator = " < ";
        break;
      case "GTE":
        sqlOperator = " >= ";
        break;
      case "LTE":
        sqlOperator = " <= ";
        break;
      case "LIKE":
        // Case insensitive regex search, Append % at beginning and end of value to do a regex
        // search
        sqlOperator = " ILIKE ";
        value = "%" + value + "%";
        break;
      case "NOT_IN":
        // NOTE: Pl. refer this in non-parsed expression for limitation of this filter
        sqlOperator = " NOT IN ";
        isMultiValued = true;
        value = prepareParameterizedStringForList((List<Object>) value, paramsBuilder);
        break;
      case "IN":
        // NOTE: Pl. refer this in non-parsed expression for limitation of this filter
        sqlOperator = " IN ";
        isMultiValued = true;
        value = prepareParameterizedStringForList((List<Object>) value, paramsBuilder);
        break;
      case "NOT_EXISTS":
        sqlOperator = " IS NULL ";
        value = null;
        break;
      case "EXISTS":
        sqlOperator = " IS NOT NULL ";
        value = null;
        break;
      case "NEQ":
        // NOTE: Pl. refer this in non-parsed expression for limitation of this filter
        sqlOperator = " != ";
        break;
      case "CONTAINS":
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

  public static List<String> splitNestedField(String nestedFieldName) {
    return Arrays.asList(StringUtils.split(nestedFieldName, DOT));
  }

  public static boolean isEncodedNestedField(String fieldName) {
    return fieldName.contains(DOT_STR) ? true : false;
  }

  public static String encodeAliasForNestedField(String nestedFieldName) {
    return StringUtils.replace(nestedFieldName, DOT, DOT_STR);
  }

  public static String decodeAliasForNestedField(String nestedFieldName) {
    return StringUtils.replace(nestedFieldName, DOT_STR, DOT);
  }
}
