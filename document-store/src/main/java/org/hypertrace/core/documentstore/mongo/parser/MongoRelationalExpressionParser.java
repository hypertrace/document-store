package org.hypertrace.core.documentstore.mongo.parser;

import static org.hypertrace.core.documentstore.Collection.UNSUPPORTED_QUERY_OPERATION;

import com.mongodb.BasicDBObject;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.hypertrace.core.documentstore.expression.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.SelectingExpression;

public class MongoRelationalExpressionParser {
  static Map<String, Object> parse(RelationalExpression expression) {
    SelectingExpression expression1 = expression.getOperand1();
    RelationalOperator operator = expression.getOperator();
    SelectingExpression expression2 = expression.getOperand2();

    MongoSelectingExpressionParser selectionParser = new MongoSelectingExpressionParser();
    String key = Objects.toString(expression1.parse(selectionParser));
    Object value = expression2.parse(selectionParser);

    return generateMap(key, value, operator);
  }

  private static Map<String, Object> generateMap(
      String key, Object value, RelationalOperator operator) {
    Map<String, Object> map = new HashMap<>();
    switch (operator) {
      case EQ:
        map.put(key, value);
        break;
      case LIKE:
        // Case insensitive regex search
        map.put(key, new BasicDBObject("$regex", value).append("$options", "i"));
        break;
      case NOT_IN:
        map.put(key, new BasicDBObject("$nin", value));
        break;
      case IN:
        map.put(key, new BasicDBObject("$in", value));
        break;
      case CONTAINS:
        map.put(key, new BasicDBObject("$elemMatch", value));
        break;
      case GT:
        map.put(key, new BasicDBObject("$gt", value));
        break;
      case LT:
        map.put(key, new BasicDBObject("$lt", value));
        break;
      case GTE:
        map.put(key, new BasicDBObject("$gte", value));
        break;
      case LTE:
        map.put(key, new BasicDBObject("$lte", value));
        break;
      case EXISTS:
        map.put(key, new BasicDBObject("$exists", true));
        break;
      case NOT_EXISTS:
        map.put(key, new BasicDBObject("$exists", false));
        break;
      case NEQ:
        // $ne operator in Mongo also returns the results, where the key does not exist in the
        // document. This is as per semantics of EQ vs NEQ. So, if you need documents where
        // key exists, consumer needs to add additional filter.
        // https://github.com/hypertrace/document-store/pull/20#discussion_r547101520
        map.put(key, new BasicDBObject("$ne", value));
        break;
      default:
        throw new UnsupportedOperationException(UNSUPPORTED_QUERY_OPERATION);
    }
    return map;
  }
}
