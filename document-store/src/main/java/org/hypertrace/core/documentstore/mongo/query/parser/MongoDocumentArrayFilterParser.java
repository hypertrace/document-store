package org.hypertrace.core.documentstore.mongo.query.parser;

import static java.util.Map.entry;
import static org.hypertrace.core.documentstore.expression.operators.ArrayOperator.ANY;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;
import org.hypertrace.core.documentstore.expression.impl.DocumentArrayFilterExpression;
import org.hypertrace.core.documentstore.expression.operators.ArrayOperator;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.mongo.MongoUtils;

class MongoDocumentArrayFilterParser {
  private static final String EXPR = "$expr";
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

  private final UnaryOperator<MongoSelectTypeExpressionParser> wrappingLhsParser;
  private final boolean exprTypeFilter;

  MongoDocumentArrayFilterParser(
      final UnaryOperator<MongoSelectTypeExpressionParser> wrappingLhsParser,
      final boolean exprTypeFilter) {
    this.wrappingLhsParser = wrappingLhsParser;
    this.exprTypeFilter = exprTypeFilter;
  }

  Map<String, Object> parse(final DocumentArrayFilterExpression arrayFilterExpression) {
    final String operator =
        Optional.ofNullable(OPERATOR_MAP.get(arrayFilterExpression.getOperator()))
            .orElseThrow(
                () ->
                    new UnsupportedOperationException(
                        "Unsupported array operator in " + arrayFilterExpression));

    final SelectTypeExpression arraySource = arrayFilterExpression.getArraySource();
    final String arraySourceName = arraySource.accept(identifierParser);
    final String alias = MongoUtils.getLastField(arraySourceName);

    /*
     * Wrapping parser to convert 'lhs' to '$$prefix.lhs' in the case of nested array filters.
     * Dollar prefixing idempotent parser to retain '$$prefix.lhs' to '$$prefix.lhs' in the case of nested array filters.
     * In the case of non-nested array filters, 'lhs' will just be converted to '$lhs' by the dollar prefixing idempotent parser
     */
    final MongoSelectTypeExpressionParser wrappingParser =
        new MongoDollarPrefixingIdempotentParser(wrappingLhsParser.apply(identifierParser));
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
                  "$planets",
                  []
                ]
              },
              "as": "planets",
              "in":
              {
                "$eq": ["$$planets.name", "Mars"]
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
                    parser -> buildPrefixingParser(alias, parser), true));

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
    return exprTypeFilter ? arrayFilter : Map.of(EXPR, arrayFilter);
  }

  private MongoIdentifierPrefixingParser buildPrefixingParser(
      final String alias, final MongoSelectTypeExpressionParser baseParser) {
    // Substitute the array name in the LHS with the alias (because it could be encoded)
    // and then wrap with dollar ($) twice. E.g.: 'name' --> '$$planets.name'
    return new MongoIdentifierPrefixingParser(
        new MongoIdentifierPrefixingParser(
            new MongoIdentifierPrefixingParser(baseParser, alias + ".")));
  }
}
