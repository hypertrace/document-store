package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;

class PostgresContainsRelationalFilterParser
    implements PostgresContainsRelationalFilterParserInterface {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public String parse(
      final RelationalExpression expression, final PostgresRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    final Object parsedRhs = expression.getRhs().accept(context.rhsParser());

    final Object convertedRhs = prepareJsonValueForContainsOp(parsedRhs);
    context.getParamsBuilder().addObjectParam(convertedRhs);

    return String.format("%s @> ?::jsonb", parsedLhs);
  }

  static String prepareJsonValueForContainsOp(final Object value) {
    if (value instanceof Document) {
      return prepareDocumentValueForContainsOp((Document) value);
    } else {
      return prepareValueForContainsOp(value);
    }
  }

  private static String prepareDocumentValueForContainsOp(final Document document) {
    try {
      final JsonNode node = OBJECT_MAPPER.readTree(document.toJson());
      if (node.isArray()) {
        return document.toJson();
      } else {
        return "[" + document.toJson() + "]";
      }
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private static String prepareValueForContainsOp(final Object value) {
    try {
      if (value instanceof Iterable<?>) {
        return OBJECT_MAPPER.writeValueAsString(value);
      } else {
        return "[" + OBJECT_MAPPER.writeValueAsString(value) + "]";
      }
    } catch (JsonProcessingException ex) {
      throw new RuntimeException(ex);
    }
  }
}
