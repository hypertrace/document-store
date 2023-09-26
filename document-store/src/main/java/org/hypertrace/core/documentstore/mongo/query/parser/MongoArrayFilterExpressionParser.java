package org.hypertrace.core.documentstore.mongo.query.parser;

import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.ArrayFilterExpression;

/**
 * [ { "colors": [ "V", "I", "B", "G", "Y", "O", "R" ], "nested": [ { "planet": "Mars", "colors": []
 * }, { "planet": "Earth", "colors": [ "Blue" ] } ] }, { "colors": [ "R", "G", "A", "K" ], "nested":
 * [ { "planet": "Jupiter", "colors": [ "Brown", "Blue" ] }, { "planet": "Earth", "colors": [ "Blue"
 * ] } ] } ]
 */
final class MongoArrayFilterExpressionParser {
  Map<String, Object> parse(final ArrayFilterExpression expression) {
    switch (expression.getArrayOperator()) {
      case ALL:
        // "$anyElementsTrue"
        break;
      case ANY:
        /**
         * db.collection.aggregate([ { $match: { $expr: { $allElementsTrue: { $map: { input:
         * "$colors", as: "elem", in: { $gte: [ "$$elem", "B" ] } } } } } } ])
         *
         * <p>select str from tes WHERE NOT EXISTS (SELECT 1 FROM
         * jsonb_array_elements(str->'colors') AS elem WHERE NOT (trim('"' FROM elem::text) >=
         * 'B'));
         */

        /**
         * db.collection.aggregate([ { $match: { $expr: { $anyElementTrue: { $map: { input:
         * "$nested", as: "elem", in: { $eq: [ "$$elem.color", "Blue" ] } } } } } } ])
         *
         * <p>select str from tes WHERE EXISTS (SELECT 1 FROM jsonb_array_elements(str->'nested') AS
         * elem WHERE elem->>'color' = 'Blue');
         *
         * <p>db.collection.aggregate([ { $match: { $expr: { $anyElementTrue: { $map: { input:
         * "$nested", as: "elem", in: { "$anyElementTrue": { $map: { input: "$$elem.colors", as:
         * "e", in: { "$or": [ { "$eq": [ "$$e", "Brown" ] }, { "$eq": [ "$$e", "Blue" ] } ] } } } }
         * } } } } } ]) select str from tes WHERE EXISTS (SELECT 1 FROM
         * jsonb_array_elements(str->'nested') AS elem WHERE EXISTS (SELECT 1 FROM
         * jsonb_array_elements(elem->'colors') AS e WHERE TRIM('"' FROM e::text) = 'Brown' OR
         * TRIM('"' FROM e::text) = 'Blue'));
         */
    }

    throw new UnsupportedOperationException();
  }
}
