package org.hypertrace.core.documentstore.postgres.utils;

import static java.util.Objects.requireNonNull;
import static org.hypertrace.core.documentstore.Collection.UNSUPPORTED_QUERY_OPERATION;
import static org.hypertrace.core.documentstore.commons.DocStoreConstants.IMPLICIT_ID;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.CREATED_AT;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.DOCUMENT;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.DOC_PATH_SEPARATOR;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.ID;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.UPDATED_AT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.Filter.Op;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.postgres.Params.Builder;
import org.hypertrace.core.documentstore.postgres.model.DocumentAndId;

@Slf4j
public class PostgresUtils {
  public static final String JSON_FIELD_ACCESSOR = "->";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String QUESTION_MARK = "?";
  private static final String JSON_DATA_ACCESSOR = "->>";
  private static final String DOT_STR = "_dot_";
  private static final String DOT = ".";

  public static final Set<String> OUTER_COLUMNS =
      new TreeSet<>(List.of(ID, CREATED_AT, UPDATED_AT));
  public static final String DOCUMENT_COLUMN = DOCUMENT;

  public enum Type {
    STRING,
    BOOLEAN,
    NUMERIC,
    STRING_ARRAY,
  }

  public static StringBuilder prepareFieldAccessorExpr(String fieldName, String columnName) {
    // Generate json field accessor statement
    if (!OUTER_COLUMNS.contains(fieldName)) {
      StringBuilder filterString = new StringBuilder(columnName);
      String[] nestedFields = fieldName.split(DOC_PATH_SEPARATOR);
      for (String nestedField : nestedFields) {
        filterString.append(JSON_FIELD_ACCESSOR).append("'").append(nestedField).append("'");
      }
      return filterString;
    }
    // There is no need of field accessor in case of outer column.
    return new StringBuilder(fieldName);
  }

  /**
   * Add field prefix for searching into json document based on postgres syntax, handles nested
   * keys. Note: It doesn't handle array elements in json document. e.g SELECT * FROM TABLE where
   * document ->> 'first' = 'name' and document -> 'address' ->> 'pin' = "00000"
   */
  public static String prepareFieldDataAccessorExpr(String fieldName, String columnName) {
    StringBuilder fieldPrefix = new StringBuilder(fieldName);
    if (!OUTER_COLUMNS.contains(fieldName)) {
      fieldPrefix = new StringBuilder(columnName);
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

  public static Type getType(Object value) {
    boolean isArrayType = false;
    Type type = Type.STRING;

    // handle the case if the value type is collection for filter operator - `IN`
    // Currently, for `IN` operator, we are considering List collection, and it is fair
    // assumption that all its value of the same types. Based on that and for consistency
    // we will use CAST ( <field name> as <type> ) for all non string operator.
    // Ref : https://github.com/hypertrace/document-store/pull/30#discussion_r571782575
    if (value instanceof List<?> && ((List<Object>) value).size() > 0) {
      List<Object> listValue = (List<Object>) value;
      value = listValue.get(0);
      isArrayType = true;
    }

    if (value instanceof Number) {
      type = Type.NUMERIC;
    } else if (value instanceof Boolean) {
      type = Type.BOOLEAN;
    } else if (isArrayType) {
      type = Type.STRING_ARRAY;
    }
    return type;
  }

  public static String prepareCast(String field, Object value) {
    return prepareCast(field, getType(value));
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

  public static String prepareCastForFieldAccessor(String field, Object value) {
    String fmt = "CAST (%s AS %s)";
    Type type = getType(value);
    if (type.equals(Type.STRING) || type.equals(Type.STRING_ARRAY))
      return String.format(fmt, field, "TEXT");
    return prepareCast(field, type);
  }

  public static Object preProcessedStringForFieldAccessor(Object value) {
    Type type = getType(value);
    if (type.equals(Type.STRING)) {
      return PostgresUtils.wrapAliasWithDoubleQuotes((String) value);
    } else if (type.equals(Type.STRING_ARRAY)) {
      return ((List<Object>) value)
          .stream()
              .map(Object::toString)
              .map(strValue -> PostgresUtils.wrapAliasWithDoubleQuotes(strValue))
              .collect(Collectors.toUnmodifiableList());
    }
    return value;
  }

  public static String prepareParameterizedStringForList(
      Iterable<Object> values, Params.Builder paramsBuilder) {
    String collect =
        StreamSupport.stream(values.spliterator(), false)
            .map(
                val -> {
                  paramsBuilder.addObjectParam(val);
                  return QUESTION_MARK;
                })
            .collect(Collectors.joining(", "));
    return "(" + collect + ")";
  }

  public static String prepareParameterizedStringForJsonList(
      Iterable<Object> values, Params.Builder paramsBuilder) {
    String collect =
        StreamSupport.stream(values.spliterator(), false)
            .map(
                val -> {
                  paramsBuilder.addObjectParam(val);
                  return QUESTION_MARK + "::jsonb";
                })
            .collect(Collectors.joining(", "));
    return "(" + collect + ")";
  }

  public static String parseNonCompositeFilterWithCasting(
      String fieldName, String columnName, String op, Object value, Builder paramsBuilder) {
    String parsedExpression =
        prepareCast(prepareFieldDataAccessorExpr(fieldName, columnName), value);
    return parseNonCompositeFilter(
        fieldName, parsedExpression, columnName, op, value, paramsBuilder);
  }

  @SuppressWarnings("unchecked")
  public static String parseNonCompositeFilter(
      String fieldName,
      String parsedExpression,
      String columnName,
      String op,
      Object value,
      Builder paramsBuilder) {
    StringBuilder filterString = new StringBuilder(parsedExpression);
    String sqlOperator;
    boolean isMultiValued = false;
    boolean isContainsOp = false;
    switch (op) {
      case "EQ":
      case "=":
        // For non-primitive object types, the behaviour of equality is element match in mongo
        // So, to handle compatibility, For postgres, we are mapping it to contains operator.
        // Refer the test case {@link DocStoreTest.test_ArrayValue_Total}
        // TODO: Change client to use contains operator
        if (isNotIterableAndFilterObjectType(value)) {
          return parseNonCompositeFilter(
              fieldName,
              parsedExpression,
              columnName,
              Op.CONTAINS.toString(),
              value,
              paramsBuilder);
        }
        sqlOperator = " = ";
        break;
      case "GT":
      case ">":
        sqlOperator = " > ";
        break;
      case "LT":
      case "<":
        sqlOperator = " < ";
        break;
      case "GTE":
      case ">=":
        sqlOperator = " >= ";
        break;
      case "LTE":
      case "<=":
        sqlOperator = " <= ";
        break;
      case "LIKE":
      case "~":
        // Case insensitive regex search, Append % at beginning and end of value to do a regex
        // search
        sqlOperator = " ~* ";
        value = ".*" + value + ".*";
        break;
      case "NOT_IN":
      case "NOT IN":
        // NOTE: Below two points
        // 1. both NOT_IN and IN filter currently limited to non-array field
        //    - https://github.com/hypertrace/document-store/issues/32#issuecomment-781411676
        // 2. To make semantically opposite filter of IN, we need to check for if key is not present
        //    Ref in context of NEQ -
        // https://github.com/hypertrace/document-store/pull/20#discussion_r547101520Other
        //    so, we need - "document->key IS NULL OR document->key->> NOT IN (v1, v2)"
        StringBuilder notInFilterString = prepareFieldAccessorExpr(fieldName, columnName);
        if (!OUTER_COLUMNS.contains(fieldName)) {
          filterString = notInFilterString.append(" IS NULL OR ").append(parsedExpression);
        }
        sqlOperator = " NOT IN ";
        isMultiValued = true;
        value = prepareParameterizedStringForList((Iterable<Object>) value, paramsBuilder);
        break;
      case "IN":
        // NOTE: both NOT_IN and IN filter currently limited to non-array field
        //  - https://github.com/hypertrace/document-store/issues/32#issuecomment-781411676
        sqlOperator = " IN ";
        isMultiValued = true;
        value = prepareParameterizedStringForList((Iterable<Object>) value, paramsBuilder);
        break;
      case "NOT_EXISTS":
      case "NOT EXISTS":
        sqlOperator = " IS NULL ";
        value = null;
        // For fields inside jsonb
        StringBuilder notExists = prepareFieldAccessorExpr(fieldName, columnName);
        if (!OUTER_COLUMNS.contains(fieldName)) {
          filterString = notExists;
        }
        break;
      case "EXISTS":
        sqlOperator = " IS NOT NULL ";
        value = null;
        // For fields inside jsonb
        StringBuilder exists = prepareFieldAccessorExpr(fieldName, columnName);
        if (!OUTER_COLUMNS.contains(fieldName)) {
          filterString = exists;
        }
        break;
      case "NEQ":
      case "!=":
        // For non-primitive object types, the behaviour of equality is element match in mongo
        // So, to handle compatibility, For postgres, we are mapping it to contains operator.
        // Refer the test case {@link DocStoreTest.test_ArrayValue_Total}
        // TODO: Change client to use contains operator
        if (isNotIterableAndFilterObjectType(value)) {
          return parseNonCompositeFilter(
              fieldName,
              parsedExpression,
              columnName,
              Op.NOT_CONTAINS.toString(),
              value,
              paramsBuilder);
        }
        sqlOperator = " != ";
        // https://github.com/hypertrace/document-store/pull/20#discussion_r547101520
        // The expected behaviour is to get all documents which either satisfy non equality
        // condition
        // or the key doesn't exist in them
        // Semantics for handling if key not exists and if it exists, its value
        // doesn't equal to the filter for Jsonb document will be done as:
        // "document->key IS NULL OR document->key->> != value"
        StringBuilder notEquals = prepareFieldAccessorExpr(fieldName, columnName);
        // For fields inside jsonb
        if (!OUTER_COLUMNS.contains(fieldName)) {
          filterString = notEquals.append(" IS NULL OR ").append(parsedExpression);
        }
        break;
      case "CONTAINS":
        // only work with array elements of jsonb document
        if (OUTER_COLUMNS.contains(fieldName)) {
          throw new UnsupportedOperationException(UNSUPPORTED_QUERY_OPERATION);
        }
        isContainsOp = true;
        filterString = prepareFieldAccessorExpr(fieldName, columnName);
        value = prepareValueForContainsOp(value);
        sqlOperator = " @> ";
        break;
      case "NOT_CONTAINS":
        // only work with array elements of jsonb document
        if (OUTER_COLUMNS.contains(fieldName)) {
          throw new UnsupportedOperationException(UNSUPPORTED_QUERY_OPERATION);
        }
        isContainsOp = true;
        String lhsExp = prepareFieldAccessorExpr(fieldName, columnName).toString();
        filterString = new StringBuilder(lhsExp).append(" IS NULL OR NOT ").append(lhsExp);
        value = prepareValueForContainsOp(value);
        sqlOperator = " @> ";
        break;
      default:
        throw new UnsupportedOperationException(UNSUPPORTED_QUERY_OPERATION);
    }

    filterString.append(sqlOperator);
    if (value != null) {
      if (isMultiValued) {
        filterString.append(value);
      } else if (isContainsOp) {
        filterString.append(QUESTION_MARK);
        filterString.append("::jsonb");
        paramsBuilder.addObjectParam(value);
      } else {
        filterString.append(QUESTION_MARK);
        paramsBuilder.addObjectParam(value);
      }
    }
    return filterString.toString();
  }

  private static StringBuilder prepareFilterStringForInOperator(
      final String parsedExpression, final Iterable<Object> values, final Builder paramsBuilder) {
    final StringBuilder filterStringBuilder = new StringBuilder();
    filterStringBuilder.append("(");
    final String collect =
        StreamSupport.stream(values.spliterator(), false)
            .map(
                val -> {
                  paramsBuilder.addObjectParam(val).addObjectParam(val);
                  return String.format(
                      "((jsonb_typeof(to_jsonb(%s)) = 'array' AND to_jsonb(%s) @> jsonb_build_array(?)) OR (jsonb_build_array(%s) @> jsonb_build_array(?)))",
                      parsedExpression, parsedExpression, parsedExpression);
                })
            .collect(Collectors.joining(" OR "));
    filterStringBuilder.append(collect);
    filterStringBuilder.append(")");
    return filterStringBuilder;
  }

  private static Object prepareJsonValueForContainsOp(final Object value) {
    if (value instanceof Document) {
      try {
        final Document document = (Document) value;
        final JsonNode node = OBJECT_MAPPER.readTree(document.toJson());
        if (node.isArray()) {
          return document.toJson();
        } else {
          return "[" + document.toJson() + "]";
        }
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    } else {
      return prepareValueForContainsOp(value);
    }
  }

  private static Object prepareValueForContainsOp(Object value) {
    String transformedValue = null;
    try {
      if (value instanceof Iterable<?>) {
        transformedValue = OBJECT_MAPPER.writeValueAsString(value);
      } else {
        transformedValue = "[" + OBJECT_MAPPER.writeValueAsString(value) + "]";
      }
    } catch (JsonProcessingException ex) {
      throw new RuntimeException(ex);
    }
    return transformedValue;
  }

  /**
   * In SQL, both having clause and group by clause should match the field accessor pattern. So, as
   * part of the below method, we are only using field accessor pattern. This method will be used in
   * Having / Where clause perperation.
   *
   * <p>See the corresponding test at
   * PostgresQueryParserTest.testAggregationFilterAlongWithNonAliasFields. In the above example,
   * check how the price is accessed using -> instead of ->>.
   */
  @SuppressWarnings("unchecked")
  public static String prepareParsedNonCompositeFilter(
      String preparedExpression, String op, Object value, Builder paramsBuilder) {
    // TODO : combine this method with parseNonCompositeFilter
    StringBuilder filterString = new StringBuilder(preparedExpression);
    String sqlOperator;
    boolean isMultiValued = false;
    boolean isContainsOp = false;
    switch (op) {
      case "EQ":
      case "=":
        sqlOperator = " = ";
        break;
      case "GT":
      case ">":
        sqlOperator = " > ";
        break;
      case "LT":
      case "<":
        sqlOperator = " < ";
        break;
      case "GTE":
      case ">=":
        sqlOperator = " >= ";
        break;
      case "LTE":
      case "<=":
        sqlOperator = " <= ";
        break;
      case "LIKE":
      case "~":
        // Case insensitive regex search, Append % at beginning and end of value to do a regex
        // search
        sqlOperator = " ~* ";
        value = ".*" + value + ".*";
        break;
      case "NOT_IN":
      case "NOT IN":
        // In order to make the behaviour same as for Mongo, the "NOT_IN" operator would match only
        // if
        // the LHS and RHS have no intersection at all
        // NOTE: This doesn't work in case the LHS is a function
        sqlOperator = " NOT IN ";
        isMultiValued = true;
        filterString
            .append(" IS NULL OR")
            .append(" NOT (")
            .append(
                prepareFilterStringForInOperator(
                    preparedExpression, (Iterable<Object>) value, paramsBuilder))
            .append(")");
        return filterString.toString();
      case "IN":
        // In order to make the behaviour same as for Mongo, the "INI" operator would match if the
        // LHS and RHS have any intersection (i.e. non-empty intersection)
        // NOTE: Pl. refer this in non-parsed expression for limitation of this filter
        // NOTE: This doesn't work in case the LHS is a function
        sqlOperator = " IN ";
        isMultiValued = true;
        filterString =
            prepareFilterStringForInOperator(
                preparedExpression, (Iterable<Object>) value, paramsBuilder);
        return filterString.toString();
      case "NOT_EXISTS":
      case "NOT EXISTS":
        sqlOperator = " IS NULL ";
        value = null;
        break;
      case "EXISTS":
        sqlOperator = " IS NOT NULL ";
        value = null;
        break;
      case "NEQ":
      case "!=":
        // NOTE: Pl. refer this in non-parsed expression for limitation of this filter
        sqlOperator = " != ";
        break;
      case "CONTAINS":
        isContainsOp = true;
        value = prepareJsonValueForContainsOp(value);
        sqlOperator = " @> ";
        break;
      case "NOT_CONTAINS":
      case "NOT CONTAINS":
        isContainsOp = true;
        filterString.append(" IS NULL OR NOT ").append(preparedExpression);
        value = prepareJsonValueForContainsOp(value);
        sqlOperator = " @> ";
        break;
      case "STARTS_WITH":
      case "STARTS WITH":
        sqlOperator = " ^@ ";
        // Add explicit cast to string since starts with can only be applied on string fields
        filterString.append("::text");
        break;
      default:
        throw new UnsupportedOperationException(UNSUPPORTED_QUERY_OPERATION);
    }

    filterString.append(sqlOperator);
    if (value != null) {
      if (isMultiValued) {
        filterString.append(value);
      } else if (isContainsOp) {
        filterString.append(QUESTION_MARK);
        filterString.append("::jsonb");
        paramsBuilder.addObjectParam(value);
      } else {
        filterString.append(QUESTION_MARK);
        paramsBuilder.addObjectParam(value);
      }
    }
    return filterString.toString();
  }

  public static List<String> splitNestedField(String nestedFieldName) {
    return Arrays.asList(StringUtils.split(nestedFieldName, DOT));
  }

  public static boolean isEncodedNestedField(String fieldName) {
    return fieldName.contains(DOT_STR) || fieldName.contains(DOT);
  }

  public static String encodeAliasForNestedField(String nestedFieldName) {
    return StringUtils.replace(nestedFieldName, DOT, DOT_STR);
  }

  public static String decodeAliasForNestedField(String nestedFieldName) {
    return StringUtils.replace(nestedFieldName, DOT_STR, DOT);
  }

  public static String wrapAliasWithDoubleQuotes(String fieldName) {
    return "\"" + fieldName + "\"";
  }

  public static String wrapFieldNamesWithDoubleQuotes(String fieldName) {
    return "\"" + fieldName + "\"";
  }

  public static String formatSubDocPath(String subDocPath) {
    return "{" + subDocPath.replaceAll(DOC_PATH_SEPARATOR, ",") + "}";
  }

  public static boolean isValidPrimitiveType(Object v) {
    Set<Class<?>> validClassez =
        new HashSet<>() {
          {
            add(Double.class);
            add(Float.class);
            add(Integer.class);
            add(Long.class);
            add(String.class);
            add(Boolean.class);
            add(Number.class);
          }
        };
    return validClassez.contains(v.getClass());
  }

  private static boolean isNotIterableAndFilterObjectType(Object value) {
    if (!isValidPrimitiveType(value)
        && !(value instanceof Iterable<?>)
        && !(value instanceof Filter)
        && !(value instanceof org.hypertrace.core.documentstore.query.Filter)) {
      return true;
    }
    return false;
  }

  public static DocumentAndId extractAndRemoveId(final Document document) throws IOException {
    @SuppressWarnings("Convert2Diamond")
    final Map<String, Object> map =
        new ObjectMapper()
            .readValue(document.toJson(), new TypeReference<Map<String, Object>>() {});
    final String id = String.valueOf(requireNonNull(map.remove(IMPLICIT_ID)));
    final Document documentWithoutId = new JSONDocument(new ObjectMapper().writeValueAsString(map));
    return new DocumentAndId(documentWithoutId, id);
  }

  /**
   * Infers PostgreSQL SQL type name for createArrayOf() from Java array values. Inspects the first
   * element to determine the appropriate type.
   *
   * <p><b>Note:</b> Empty arrays should be handled by the caller before SQL generation to avoid
   * PostgreSQL type mismatch errors (e.g., {@code integer = text[]}). Filter parsers should return
   * {@code "1 = 0"} for empty IN clauses instead of calling this method.
   *
   * @param values Array of values to infer type from
   * @return PostgreSQL internal type name: "int4", "float8", "bool", or "text"
   */
  public static String inferSqlTypeFromValue(Object[] values) {
    if (values.length == 0) {
      return "text";
    }

    Object firstValue = values[0];
    if (firstValue instanceof Integer || firstValue instanceof Long) {
      return "int4";
    } else if (firstValue instanceof Double || firstValue instanceof Float) {
      return "float8";
    } else if (firstValue instanceof Boolean) {
      return "bool";
    } else {
      return "text";
    }
  }

  public static void enrichPreparedStatementWithParams(
      final PreparedStatement preparedStatement, final Params params) {
    params
        .getObjectParams()
        .forEach(
            (k, v) -> {
              try {
                if (v instanceof Params.ArrayParam) {
                  // Handle array parameter - create PostgreSQL array
                  Params.ArrayParam arrayParam = (Params.ArrayParam) v;
                  Array sqlArray =
                      preparedStatement
                          .getConnection()
                          .createArrayOf(arrayParam.getSqlType(), arrayParam.getValues());
                  preparedStatement.setArray(k, sqlArray);
                } else if (isValidPrimitiveType(v)) {
                  preparedStatement.setObject(k, v);
                } else {
                  throw new UnsupportedOperationException("Un-supported object types in filter");
                }
              } catch (SQLException e) {
                log.error("SQLException setting Param. key: {}, value: {}", k, v);
              }
            });
    if (log.isDebugEnabled()) {
      log.debug("Executing statement - preparedStatement: {}", preparedStatement);
    }
  }
}
