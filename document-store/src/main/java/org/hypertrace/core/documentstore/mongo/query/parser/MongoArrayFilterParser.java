package org.hypertrace.core.documentstore.mongo.query.parser;

import static java.util.Map.entry;
import static org.hypertrace.core.documentstore.expression.operators.ArrayOperator.ANY;
import static org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactory.FilterLocation.INSIDE_EXPR;
import static org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoStandardExprRelationalFilterParser.EXPR;

import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.documentstore.expression.impl.ArrayFilterExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.ArrayOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.mongo.MongoUtils;
import org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactory.MongoRelationalFilterContext;

class MongoArrayFilterParser {
  private static final String ANY_ELEMENT_TRUE = "$anyElementTrue";
  private static final String MAP = "$map";
  private static final String INPUT = "input";
  private static final String IF_NULL = "$ifNull";
  private static final String AS = "as";
  private static final String IN = "in";
  private static final String SET_IS_SUBSET = "$setIsSubset";
  private static final String SIZE = "$size";
  private static final String EQ = "$eq";
  private static final String IN_OPERATOR = "$in";
  private static final String ARRAY_ELEM_AT = "$arrayElemAt";
  private static final String AND = "$and";

  private static final Map<ArrayOperator, String> OPERATOR_MAP =
      Maps.immutableEnumMap(Map.ofEntries(entry(ANY, ANY_ELEMENT_TRUE)));

  private final MongoSelectTypeExpressionParser identifierParser =
      new MongoIdentifierExpressionParser();

  private final MongoRelationalFilterContext relationalFilterContext;
  private final MongoArrayFilterParserGetter arrayFilterParserWrapper;

  MongoArrayFilterParser(
      final MongoRelationalFilterContext relationalFilterContext,
      final MongoArrayFilterParserGetter arrayFilterParserWrapper) {
    this.relationalFilterContext = relationalFilterContext;
    this.arrayFilterParserWrapper = arrayFilterParserWrapper;
  }

  Map<String, Object> parse(final ArrayFilterExpression arrayFilterExpression) {
    switch (arrayFilterExpression.getOperator()) {
      case ALL:
        return parseAllOperator(arrayFilterExpression);
      case ONE:
        return parseOneOperator(arrayFilterExpression);
      default:
        return parseAnyOperator(arrayFilterExpression);
    }
  }

  private Map<String, Object> parseAnyOperator(final ArrayFilterExpression arrayFilterExpression) {
    final String operator =
        Optional.ofNullable(OPERATOR_MAP.get(arrayFilterExpression.getOperator()))
            .orElseThrow(
                () ->
                    new UnsupportedOperationException(
                        "Unsupported array operator in " + arrayFilterExpression));

    final SelectTypeExpression arraySource = arrayFilterExpression.getArraySource();
    final String sourcePath = arraySource.accept(identifierParser);
    final String alias = MongoUtils.getLastField(sourcePath);

    /*
     * Wrapping parser to convert 'lhs' to '$$prefix.lhs' in the case of nested array filters.
     * Dollar prefixing idempotent parser to retain '$$prefix.lhs' to '$$prefix.lhs' in the case of nested array filters.
     * In the case of non-nested array filters, 'lhs' will just be converted to '$lhs' by the dollar prefixing idempotent parser
     */
    final MongoSelectTypeExpressionParser wrappingParser =
        new MongoDollarPrefixingIdempotentParser(relationalFilterContext.lhsParser());
    final String mapInput = arraySource.accept(wrappingParser);

    /*
    {
      "$expr": {
        "$anyElementTrue":
          {
            "$map":
            {
              "input":
              {
                "$ifNull": [
                  "$elements",
                  []
                ]
              },
              "as": "elements",
              "in":
              {
                "$eq": ["$$elements", "Water"]
              }
            }
          }
        }
      }
     */

    final Object filter =
        arrayFilterExpression
            .getFilter()
            .accept(
                new MongoFilterTypeExpressionParser(
                    MongoRelationalFilterContext.builder()
                        .lhsParser(arrayFilterParserWrapper.getParser(sourcePath, alias))
                        .location(INSIDE_EXPR)
                        .build()));

    final Map<String, Object> arrayFilter =
        Map.of(
            operator,
            Map.of(
                MAP,
                Map.ofEntries(
                    entry(INPUT, Map.of(IF_NULL, new Object[] {mapInput, new Object[0]})),
                    entry(AS, alias),
                    entry(IN, filter))));
    return wrapInExprIfNeeded(arrayFilter);
  }

  /*
  {
    "$expr": {
      "$setIsSubset": [
        ["Blue", "Green"],
        { "$ifNull": ["$colors", []] }
      ]
    }
  }
   */
  private Map<String, Object> parseAllOperator(final ArrayFilterExpression arrayFilterExpression) {
    final Object mapInput = getDollarPrefixedArraySource(arrayFilterExpression);
    final List<?> values = getFilterValues(arrayFilterExpression);

    final Map<String, Object> setIsSubset =
        Map.of(
            SET_IS_SUBSET,
            List.of(values, Map.of(IF_NULL, new Object[] {mapInput, new Object[0]})));
    return wrapInExprIfNeeded(setIsSubset);
  }

  /*
  {
    "$expr": {
      "$and": [
        { "$eq": [{ "$size": { "$ifNull": ["$colors", []] } }, 1] },
        { "$in": [{ "$arrayElemAt": [{ "$ifNull": ["$colors", []] }, 0] }, ["Blue", "Green"]] }
      ]
    }
  }
   */
  private Map<String, Object> parseOneOperator(final ArrayFilterExpression arrayFilterExpression) {
    final Object mapInput = getDollarPrefixedArraySource(arrayFilterExpression);
    final List<?> values = getFilterValues(arrayFilterExpression);
    final Map<String, Object> arrayWithDefault =
        Map.of(IF_NULL, new Object[] {mapInput, new Object[0]});

    final Map<String, Object> sizeIsOne = Map.of(EQ, List.of(Map.of(SIZE, arrayWithDefault), 1));
    final Map<String, Object> firstElementMatches =
        Map.of(IN_OPERATOR, List.of(Map.of(ARRAY_ELEM_AT, List.of(arrayWithDefault, 0)), values));

    return wrapInExprIfNeeded(Map.of(AND, List.of(sizeIsOne, firstElementMatches)));
  }

  private String getDollarPrefixedArraySource(final ArrayFilterExpression arrayFilterExpression) {
    final MongoSelectTypeExpressionParser wrappingParser =
        new MongoDollarPrefixingIdempotentParser(relationalFilterContext.lhsParser());
    return arrayFilterExpression.getArraySource().accept(wrappingParser);
  }

  private List<?> getFilterValues(final ArrayFilterExpression arrayFilterExpression) {
    final FilterTypeExpression filter = arrayFilterExpression.getFilter();
    if (!(filter instanceof RelationalExpression)) {
      throw new UnsupportedOperationException(
          "Array operator "
              + arrayFilterExpression.getOperator()
              + " only supports a relational filter with a constant list of values, got: "
              + filter);
    }

    final SelectTypeExpression rhs = ((RelationalExpression) filter).getRhs();
    if (!(rhs instanceof ConstantExpression)) {
      throw new UnsupportedOperationException(
          "Array operator "
              + arrayFilterExpression.getOperator()
              + " requires a constant list of values, got: "
              + rhs);
    }

    final Object value = ((ConstantExpression) rhs).getValue();
    return value instanceof List ? (List<?>) value : List.of(value);
  }

  private Map<String, Object> wrapInExprIfNeeded(final Map<String, Object> filter) {
    // If already wrapped inside `$expr` avoid wrapping again
    return INSIDE_EXPR.equals(relationalFilterContext.location()) ? filter : Map.of(EXPR, filter);
  }
}
