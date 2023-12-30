package org.hypertrace.core.documentstore.mongo.query.parser;

import static java.util.Map.entry;
import static org.hypertrace.core.documentstore.expression.operators.ArrayOperator.ANY;
import static org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactory.FilterLocation.INSIDE_EXPR;
import static org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoStandardExprRelationalFilterParser.EXPR;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.documentstore.expression.impl.ArrayFilterExpression;
import org.hypertrace.core.documentstore.expression.operators.ArrayOperator;
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
    // If already wrapped inside `$expr` avoid wrapping again
    return INSIDE_EXPR.equals(relationalFilterContext.location())
        ? arrayFilter
        : Map.of(EXPR, arrayFilter);
  }
}
